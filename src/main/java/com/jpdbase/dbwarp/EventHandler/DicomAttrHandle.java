package com.jpdbase.dbwarp.EventHandler;

import com.jpdbase.dbwarp.DbInfo;

public class DicomAttrHandle extends AbstractHandler {

    public static final String TABLENAME = "dicomattr";

    private final String INSERTCOMMANDTEXT = "insert into   `dicomattr`(`studyuid`,`seriesuid`,`sopuid`,`sopclassuid`,`hiscode`,`idx`) values(?,?,?,?,?,?);";
    private final String[] INSERT_COLUMNS = {"studyuid", "seriesuid", "sopuid", "sopclassuid", "hiscode", "idx"};


    private final String DELETECOMMANDTEXT = "delete from `dicomattr` where idx = ?;";
    private final String DELETE_KEY = "idx";

    private final String UPDATECOMMANDTEXT = "update  `dicomattr`  set  `studyuid`=?,`seriesuid`=?,`sopuid`=?,`sopclassuid`=?   where idx = ? ; ";
    private final String[] UPDATE_COLUMNS = {"studyuid", "seriesuid", "sopuid", "sopclassuid", "idx"};


    public DicomAttrHandle() {
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


//    @Override
//    public void insertData(List<CanalEntry.Column> columns) {
//        try {
//            PreparedStatement stmt = connection.prepareStatement(INSERTCOMMANDTEXT);
//            stmt.clearParameters();
//            StringBuilder stringBuilder = new StringBuilder(1024);
//            stringBuilder.append(String.format("insert into %s (%s) values( ", tableName, String.join(",", INSERT_COLUMNS)));
//            for (int i = 0; i < INSERT_COLUMNS.length; i++) {
//                String cnmame = INSERT_COLUMNS[i];
//                CanalEntry.Column col = columns.parallelStream().filter(x -> StringUtils.equalsIgnoreCase(x.getName(), cnmame)).findFirst().get();
//                StatementCreatorUtils.setParameterValue(stmt, i + 1, col.getSqlType(), col.getValue());
//                final int ctype = col.getSqlType();
//                String cv = col.getValue();
//                if (       ctype == Types.INTEGER || ctype == Types.BIGINT  || ctype == Types.TINYINT || ctype == Types.SMALLINT
//                        || ctype == Types.FLOAT   || ctype == Types.DECIMAL || ctype == Types.DOUBLE || ctype == Types.REAL || ctype == Types.NUMERIC
//                        || ctype == Types.BOOLEAN || ctype == Types.BIT
//                ) {
//                    if (i == INSERT_COLUMNS.length - 1) {
//                        stringBuilder.append(String.format("%s", cv));
//                    } else {
//                        stringBuilder.append(String.format("%s,", cv));
//                    }
//                } else
//
//
////                    if( ctype == Types.CHAR || ctype == Types.VARCHAR
////                         || ctype == Types.DATE || ctype == Types.TIME || ctype == Types.TIMESTAMP
////                        || ctype == Types.NVARCHAR || ctype == Types.LONGVARCHAR  || ctype == Types.LONGNVARCHAR
////                )
//                {
//                    cv = StringUtils.replace(cv, "'", "\\'");
//
//                    if (i == INSERT_COLUMNS.length - 1) {
//                        stringBuilder.append(String.format("'%s'", cv));
//                    } else {
//                        stringBuilder.append(String.format("'%s',", cv));
//                    }
//                }
//            }
//            stringBuilder.append(");");
//            stmt.execute();
//            LOGGER.info(stringBuilder.toString());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }


}
