package com.jpdbase.dbwarp.SendMock;

import com.jpdbase.dbwarp.DbInfo;
import com.jpdbase.dbwarp.IdGenerator;
import com.mysql.jdbc.Connection;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;

public class SendData implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SendData.class);
    private final DbInfo dbInfo;
    private final int hisCode;
    private final int rows;

    public SendData(DbInfo dbInfo, int hisCode, int maxRows) {
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
                        "insert into dicomattr( studyuid, seriesuid,sopuid,sopclassuid,hiscode,idx) values (?, ?, ?,?, ?, ?);");
                //  connection.setAutoCommit(false);
                for (int i = 0; i < this.rows; i++) {
                    long idc = idGenerator.nextId();

                    String sopUid = String.format("1.2.%d", i);
                    stmt.setString(1, "1");
                    stmt.setString(2, "2");
                    stmt.setString(3, sopUid);
                    stmt.setString(4, "1.1008.1.'20");
                    stmt.setInt(5, this.hisCode);
                    stmt.setLong(6, idc);
                    try {
                        stmt.execute();
                    } catch (com.mysql.jdbc.exceptions.jdbc4.MySQLIntegrityConstraintViolationException dulicateEx) {
                        String msg = StringUtils.trim(dulicateEx.getMessage());

                        if (StringUtils.startsWith(msg, "Duplicate entry ")) {
                            LOGGER.info("重复主键,重新生成，二次尝试！");
                            idc = idGenerator.nextId();
                            stmt.setLong(6, idc);
                            stmt.execute();
                        } else {
                            LOGGER.info(msg);
                        }
                    }
                    LOGGER.info("insert into dicomattr: studyUid={} seriesUid={}  sopUid={} sopClsUid={}  hisCode ={} , idx={} ",
                            "1", "2", sopUid, "1.1008.1.20", this.hisCode, idc);
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
