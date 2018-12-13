package com.jpdbase.dbwarp;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.mysql.jdbc.Connection;
import com.sun.istack.internal.NotNull;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.StatementCreatorUtils;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Dispatch implements Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(Dispatch.class);
    private final CanalConnector connector;
    private final int batchSize = 1000;

    public Dispatch(String canalHost, int canalPort, String destination, String username, String password) {
        // 创建链接
        connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalHost, canalPort), destination, username, password);
    }

    private void printEntry(@NotNull List<CanalEntry.Entry> entrys) {
        for (CanalEntry.Entry entry : entrys) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChage = null;
            try {
                rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            CanalEntry.EventType eventType = rowChage.getEventType();
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            System.out.println(String.format("values is ===> : %s", rowChage.getSql()));

            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    printColumn(rowData.getBeforeColumnsList());
                    deleteData(
                            entry.getHeader().getTableName(),
                            rowData.getBeforeColumnsList()
                    );
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    printColumn(rowData.getAfterColumnsList());
                    insertData(
                            entry.getHeader().getTableName(),
                            rowData.getAfterColumnsList()
                    );
                } else {
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }
    }

    private void deleteData(String tabName, @NotNull List<CanalEntry.Column> columns) {
        List<String> cols = new ArrayList<>(columns.size());
        List<String> vals = new ArrayList<>(columns.size());
        int hisCode = -1;
        String idxStr = "";
        int sqlType = -1;

        for (CanalEntry.Column column : columns) {
            if (StringUtils.equalsIgnoreCase("idx", column.getName())) {
                idxStr = column.getValue();
                sqlType = column.getSqlType();
            }
            if (StringUtils.equalsIgnoreCase(column.getName(), "hiscode")) {
                hisCode = Integer.parseInt(column.getValue());
            }
        }
        if (hisCode == -1) {
            return;
        }
        if (StringUtils.isEmpty(idxStr)) {
            return;
        }

        final String sqlSta = String.format("delete from `%s` where idx = ?;", tabName);
        LOGGER.info("Delete  SQL is :{}", sqlSta);
        DbInfo db = DbWrapper.instance().getHisMapDB(hisCode);

        try {
            Connection connection = DbWrapper.instance().createConnection(db);
            PreparedStatement stmt = (com.mysql.jdbc.PreparedStatement) connection.prepareStatement(sqlSta);
            StatementCreatorUtils.setParameterValue(stmt,1, sqlType, idxStr);
            stmt.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void insertData(String tabName, @NotNull List<CanalEntry.Column> columns) {
        List<String> cols = new ArrayList<>(columns.size());
        List<String> vals = new ArrayList<>(columns.size());

        int hisCode = -1;

        for (CanalEntry.Column column : columns) {
            cols.add("`" + column.getName() + "`");
            int sqlType = column.getSqlType();
            vals.add("?");
            if (StringUtils.equalsIgnoreCase(column.getName(), "hiscode")) {
                hisCode = Integer.parseInt(column.getValue());
            }

        }
        if (hisCode == -1) {
            return;
        }
        final String cmdStr = StringUtils.join(cols, ",");
        final String prex = StringUtils.join(vals, ",");
        final String sqlSta = String.format("insert into   `%s`(%s) values(%s);", tabName, cmdStr, prex);
        LOGGER.info("Insert SQL is :{}", sqlSta);

        DbInfo db = DbWrapper.instance().getHisMapDB(hisCode);

        try {
            Connection connection = DbWrapper.instance().createConnection(db);
            PreparedStatement stmt = (com.mysql.jdbc.PreparedStatement) connection.prepareStatement(sqlSta);
            stmt.clearParameters();
            int ic = 1;
            for (CanalEntry.Column column : columns) {
                StatementCreatorUtils.setParameterValue(stmt,ic, column.getSqlType(), column.getValue());
                ic += 1;
            }
            stmt.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    private void printColumn(@NotNull List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
        }
    }


    @Override
    public void run() {
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    try {
                        connector.disconnect();
                    } catch (Exception e) {
                    }
                }
            });
            while (true) {
                Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    printEntry(message.getEntries());
                }
                connector.ack(batchId); // 提交确认
            }

        } catch (Exception ex) {

            LOGGER.error(ex.getMessage());
            ex.printStackTrace();
        }

    }
}
