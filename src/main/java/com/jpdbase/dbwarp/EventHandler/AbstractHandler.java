package com.jpdbase.dbwarp.EventHandler;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.jpdbase.dbwarp.DbInfo;
import com.jpdbase.dbwarp.IEventHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.StatementCreatorUtils;

import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.List;

public abstract class AbstractHandler implements IEventHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractHandler.class);

    protected DbInfo dbInfo;
    protected final String tableName;
    protected com.mysql.jdbc.MySQLConnection connection;

    protected AbstractHandler(String tableName) {
        this.tableName = tableName;
    }


    @Override
    public void attachTdDbInstance(DbInfo dbInfo) {
        try {

            if (this.dbInfo == null) {
                this.dbInfo = dbInfo;
                connection = dbInfo.CreateConnection();
            } else if (!StringUtils.equalsIgnoreCase(this.dbInfo.getInstanceName(), dbInfo.getInstanceName())) {
                this.dbInfo = dbInfo;
                connection = dbInfo.CreateConnection();
            }


        } catch (Exception ex) {
            LOGGER.error(ex.getMessage());
            ex.printStackTrace();
        }

    }

    protected abstract String getInsertCommandText();

    protected abstract String[] getInsertColumns();


    protected abstract String getDeleteCommandText();

    protected abstract String getDeleteKeyColumn();


    protected abstract String getUpdateCommandText();

    protected abstract String[] getUpdateColumns();

    @Override
    public void insertData(List<CanalEntry.Column> columns) {
        try {
            PreparedStatement stmt = connection.prepareStatement(getInsertCommandText());
            stmt.clearParameters();
            StringBuilder stringBuilder = new StringBuilder(1024);
            final String[] INSERT_COLUMNS = getInsertColumns();
            for (int i = 0; i < INSERT_COLUMNS.length; i++) {
                String cnmame = INSERT_COLUMNS[i];
                CanalEntry.Column col = columns.parallelStream().filter(x -> StringUtils.equalsIgnoreCase(x.getName(), cnmame)).findFirst().get();
                StatementCreatorUtils.setParameterValue(stmt, i + 1, col.getSqlType(), col.getValue());
            }
            stmt.execute();
            LOGGER.info(stringBuilder.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void updateData(List<CanalEntry.Column> beforeColumnsList, List<CanalEntry.Column> afterColumnsList) {
        try {
            PreparedStatement stmt = connection.prepareStatement(getUpdateCommandText());
            stmt.clearParameters();
            final String[] UPDATE_COLUMNS = getUpdateColumns();
            for (int i = 0; i < UPDATE_COLUMNS.length; i++) {
                String cnmame = UPDATE_COLUMNS[i];
                CanalEntry.Column col = afterColumnsList.parallelStream().filter(x -> StringUtils.equalsIgnoreCase(x.getName(), cnmame)).findFirst().get();
                StatementCreatorUtils.setParameterValue(stmt, i + 1, col.getSqlType(), col.getValue());
            }
            stmt.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void deleteData(List<CanalEntry.Column> columns) {
        try {
            PreparedStatement stmt = connection.prepareStatement(getDeleteCommandText());
            CanalEntry.Column col = columns.parallelStream().filter(x -> StringUtils.equalsIgnoreCase(x.getName(), getDeleteKeyColumn())).findFirst().get();
            StatementCreatorUtils.setParameterValue(stmt, 1, col.getSqlType(), col.getValue());
            stmt.execute();
            LOGGER.info("Delete From {} Where Idx ={}", tableName, col.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    //    private final String INSERTCOMMANDTEXT = "insert into   `dicomjson`(`idx`,`hiscode`,`studyuid`,`content`,`jsonstr`) values(?,?,?,?,?);";
//    private final String[] INSERT_COLUMNS = {"idx", "hiscode", "studyuid", "content", "jsonstr"};
//
//
//    private final String DELETECOMMANDTEXT = "delete from `dicomjson` where idx = ?;";
//    private final String DELETE_KEY = "idx";
//
//    private final String UPDATECOMMANDTEXT = "update  `dicomjson`  set  `studyuid`=?,`content`=?,`jsonstr`=?  where idx = ? ; ";
//    private final String[] UPDATE_COLUMNS = {"studyuid", "content", "jsonstr", "idx"};

}
