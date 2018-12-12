package com.jpdbase.dbwarp;



import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

/**
 * @author dhz
 */
public class DbWrapper {

    public static final Logger LOGGER = LoggerFactory.getLogger(DbWrapper.class);

    private ArrayList<DbInfo> dbInfoArrayList;

    public DbWrapper() {
        dbInfoArrayList = new ArrayList<DbInfo>(10);

    }

    private static class SingletonHolder {

        private static final DbWrapper dbWrapperInstance = new DbWrapper();
    }

    public static DbWrapper instance() {
        return SingletonHolder.dbWrapperInstance;
    }


    public void RegisterDatabase(DbInfo dbInfo) {

    }

    public void UnregisterAll() {

    }


    public void Init() {
        File cnf = new File("./conf");
        if (!cnf.exists()) {
            LOGGER.info("./conf  directory is not exists !");
            return;
        }
        Collection<File> dbs = FileUtils.listFiles(cnf, new IOFileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isFile();
            }

            @Override
            public boolean accept(File file, String s) {
                return StringUtils.equals(s, "instance.properties");
            }
        }, new IOFileFilter() {
            @Override
            public boolean accept(File file) {
                return file.isDirectory();
            }

            @Override
            public boolean accept(File file, String s) {
                return !file.isHidden();
            }
        });
        if (dbs.isEmpty()) {
            LOGGER.info("No Instance Config Foud");
            return;
        }
        for (File f : dbs) {

            DbInfo dbInfo = parseFrom(f);
            if (dbInfo == null) {
                LOGGER.info("parse  DbConfig File Failed :" + f.getAbsolutePath());
                continue;
            }
            dbInfoArrayList.add(
                    dbInfo
            );
            LOGGER.info("Load DbConfig Info :"+  dbInfo);
        }

    }

    private static final String dbHost = "dbHost";
    private static final String dbPort = "dbPort";
    private static final String dbName = "dbName";
    private static final String dbUser = "dbUser";
    private static final String dbPwd = "dbPwd";

    private DbInfo parseFrom(  File cnf) {
        Properties p = new Properties();
        DbInfo dbInfo = null;
        try {
            p.load(new FileInputStream(cnf));

            dbInfo = new DbInfo();
            dbInfo.setDbName(p.getProperty(dbName));
            dbInfo.setDbHost(p.getProperty(dbHost));
            String px = p.getProperty(dbPort);
            if (StringUtils.isEmpty(px)) {
                dbInfo.setDbPort(3306);
            } else {
                dbInfo.setDbPort(Integer.parseInt(px));
            }
            dbInfo.setUserName(p.getProperty(dbUser));
            dbInfo.setUserPwd(p.getProperty(dbPwd));

        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            dbInfo = null;

        }
        return dbInfo;
    }


}