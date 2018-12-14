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

public class DicomJsonHandle extends AbstractHandler {

    /*
    create table dicomjson(
   idx  bigint(25) not null  primary key ,
   hiscode  int(11) not null,
   studyuid  varchar(64) not null,
   content  LONGBLOB,
   jsonstr  LONGTEXT
);

     */
    public static final String TABLENAME = "dicomjson";

    private final String INSERTCOMMANDTEXT = "insert into   `dicomjson`(`idx`,`hiscode`,`studyuid`,`content`,`jsonstr`) values(?,?,?,?,?);";
    private final String[] INSERT_COLUMNS = {"idx", "hiscode", "studyuid", "content", "jsonstr"};


    private final String DELETECOMMANDTEXT = "delete from `dicomjson` where idx = ?;";
    private final String DELETE_KEY = "idx";

    private final String UPDATECOMMANDTEXT = "update  `dicomjson`  set  `studyuid`=?,`content`=?,`jsonstr`=?  where idx = ? ; ";
    private final String[] UPDATE_COLUMNS = {"studyuid", "content", "jsonstr", "idx"};


    public DicomJsonHandle() {
        super(TABLENAME);
    }


    @Override
    protected String getInsertCommandText() {
        return INSERTCOMMANDTEXT;
    }

    @Override
    protected String[] getInsertColumns() {
        return INSERT_COLUMNS;
    }

    @Override
    protected String getDeleteCommandText() {
        return DELETECOMMANDTEXT;
    }

    @Override
    protected String getDeleteKeyColumn() {
        return DELETE_KEY;
    }

    @Override
    protected String getUpdateCommandText() {
        return UPDATECOMMANDTEXT;
    }

    @Override
    protected String[] getUpdateColumns() {
        return UPDATE_COLUMNS;
    }
}
