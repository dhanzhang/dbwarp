package com.jpdbase.dbwarp;


import com.mysql.jdbc.MySQLConnection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * @author dhz
 */
public class DbWrapper {

    public static final Logger LOGGER = LoggerFactory.getLogger(DbWrapper.class);

    private final ArrayList<DbInfo> dbInfoArrayList;
    private final Map<Integer, DbInfo> dbMapInfo;
    private final Map<String, MySQLConnection> connectionMap;


    public DbWrapper() {
        dbInfoArrayList = new ArrayList<DbInfo>(10);
        dbMapInfo = new HashMap<>(10);
        connectionMap = new HashMap<>(10);

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

        File mapFile = new File("./conf/hismap.properties");
        if (!mapFile.exists()) {
            LOGGER.error("File {} Not Exists !", mapFile);
            return;
        }
        Collection<File> dbs = FileUtils.listFiles(cnf, new IOFileFilter() {
                    @Override
                    public boolean accept(File file) {
                        LOGGER.info("fileFilter:{}", file.getName());

                        if (StringUtils.equals(file.getName(), "hismap.properties")) {
                            return false;
                        }
                        return file.isFile();
                    }

                    @Override
                    public boolean accept(File file, String s) {
                        return StringUtils.equals(s, "instance.properties");
                    }
                },
                new IOFileFilter() {
                    @Override
                    public boolean accept(File file) {
                        LOGGER.info("dirFilter:{}", file);
                        return file.isDirectory();
                    }

                    @Override
                    public boolean accept(File file, String s) {
                        return true;
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
            LOGGER.info("Load DbConfig Info :" + dbInfo);
        }


        Properties hm = new Properties();
        try {
            hm.load(new FileInputStream(mapFile));
            Enumeration<?> pnanmes = hm.propertyNames();
            while (pnanmes.hasMoreElements()) {
                String hiscode = (String) pnanmes.nextElement();
                String codex = StringUtils.substring(hiscode, "hiscode.".length());
                int code = Integer.parseInt(codex);

                if (dbMapInfo.containsKey(code)) {
                    LOGGER.info("{} 已经拥有对应的DB:{}", hiscode, dbMapInfo.get(code));
                    continue;
                }
                String dbInstance = hm.getProperty(hiscode);

                if (!dbInfoArrayList.parallelStream().anyMatch(x -> StringUtils.equals(dbInstance, x.getInstanceName()))) {
                    LOGGER.info("{} 没有对应的DB:{}", hiscode, dbInstance);
                    continue;
                }
                DbInfo dbx = dbInfoArrayList.parallelStream().filter(x -> StringUtils.equals(dbInstance, x.getInstanceName())).findFirst().get();
                dbMapInfo.put(code, dbx);
                LOGGER.info("{} ==>{}", hiscode, dbx);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

    private static final String dbHost = "dbHost";
    private static final String dbPort = "dbPort";
    private static final String dbName = "dbName";
    private static final String dbUser = "dbUser";
    private static final String dbPwd = "dbPwd";
    private static final String isMaster = "isMaster";

    private DbInfo parseFrom(File cnf) {
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
            String master = p.getProperty(isMaster);
            if (StringUtils.isEmpty(master)) {
                dbInfo.setMaster(false);
            } else {
                dbInfo.setMaster(Boolean.parseBoolean(master));
            }

            dbInfo.setInstanceName(cnf.getParentFile().getName());
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            dbInfo = null;
        }
        return dbInfo;
    }


    public DbInfo getHisMapDB(int hiscode) {

        if (!dbMapInfo.containsKey(hiscode)) {
            return null;
        }
        return dbMapInfo.get(hiscode);
    }

    public com.mysql.jdbc.MySQLConnection createConnection(DbInfo dbInfo) {
        final String instName = dbInfo.getInstanceName();
        if (!connectionMap.containsKey(instName)) {
            try {
                connectionMap.put(instName, dbInfo.CreateConnection());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return connectionMap.get(instName);
    }

    public DbInfo MasterDb() {

        if (!dbInfoArrayList.parallelStream().anyMatch(x -> x.isMaster())) {
            return null;
        }

        return dbInfoArrayList.parallelStream().filter(x -> x.isMaster()).findFirst().get();
    }


}