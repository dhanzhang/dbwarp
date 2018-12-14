package com.jpdbase.dbwarp.SendMock;

import com.jpdbase.dbwarp.DbInfo;
import com.jpdbase.dbwarp.IdGenerator;
import com.mysql.jdbc.Connection;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;

public class SendJson implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendData.class);
    private final DbInfo dbInfo;
    private final int hisCode;
    private final int rows;

    public SendJson(DbInfo dbInfo, int hisCode, int maxRows) {
        this.dbInfo = dbInfo;
        this.hisCode = hisCode;
        this.rows = maxRows;
    }

    @Override
    public void run() {

        try {
            Connection connection = this.dbInfo.CreateConnection();
            IdGenerator idGenerator = new IdGenerator(this.hisCode);
            try {
                PreparedStatement stmt = (com.mysql.jdbc.PreparedStatement) connection.prepareStatement(
                        "insert into dicomjson( idx,hiscode,studyuid,content, jsonstr) values (?, ?, ?,?,?);");
                FileInputStream fileInputStream = new FileInputStream(new File("./logx.png"));
                String txt = FileUtils.readFileToString(new File("./README.txt"), "UTF-8");


                for (int i = 0; i < this.rows; i++) {
                    long idc = idGenerator.nextId();

                    String uuid = UUID.randomUUID().toString();
                    stmt.setLong(1, idc);
                    stmt.setInt(2, hisCode);
                    stmt.setString(3, uuid);
                    stmt.setBlob(4, fileInputStream);
                    stmt.setString(5, txt);
                    stmt.execute();

                }
                //  connection.commit();
            } catch (Exception ex) {
                // connection.rollback();
                ex.printStackTrace();
                LOGGER.error(ex.getMessage());
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
        }

    }
}
