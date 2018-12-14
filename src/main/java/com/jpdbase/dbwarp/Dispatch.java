package com.jpdbase.dbwarp;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.mysql.jdbc.Connection;
import com.sun.istack.internal.NotNull;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.StatementCreatorUtils;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.*;

public class Dispatch implements Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(Dispatch.class);
    private final CanalConnector connector;
    private final int batchSize = 1000;

    public Dispatch(String canalHost, int canalPort, String destination, String username, String password) {
        // 创建链接
        connector = CanalConnectors.newSingleConnector(new InetSocketAddress(canalHost, canalPort), destination, username, password);
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

            String tabName = entry.getHeader().getTableName().toLowerCase();

            if (!Arrays.asList(EventHandlerFactory.SUPPORTED_TALBES).parallelStream().anyMatch(x -> StringUtils.equalsIgnoreCase(x, tabName))) {
                for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                    printColumn(rowData.getAfterColumnsList());
                }
                return;
            }

            IEventHandler handler = EventHandlerFactory.getFactoryInstance().BuilderHandler(tabName);
            for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE || eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                    CanalEntry.Column hcol;
                    if (eventType == CanalEntry.EventType.INSERT || eventType == CanalEntry.EventType.UPDATE) {
                        hcol = rowData.getAfterColumnsList().parallelStream()
                                .filter(x -> StringUtils.equalsIgnoreCase(x.getName(), EventHandlerFactory.PARTICAL_KEY)).findFirst().get();
                    } else {
                        hcol = rowData.getBeforeColumnsList().parallelStream()
                                .filter(x -> StringUtils.equalsIgnoreCase(x.getName(), EventHandlerFactory.PARTICAL_KEY)).findFirst().get();
                    }
                    final int hiscode = Integer.parseInt(hcol.getValue());
                    DbInfo dbInfo = DbWrapper.instance().getHisMapDB(hiscode);
                    handler.attachTdDbInstance(dbInfo);
                    if (eventType == CanalEntry.EventType.INSERT) {
                        handler.insertData(rowData.getAfterColumnsList());
                    } else if (eventType == CanalEntry.EventType.UPDATE) {
                        handler.updateData(rowData.getBeforeColumnsList(), rowData.getAfterColumnsList());
                    } else if (eventType == CanalEntry.EventType.DELETE) {
                        handler.deleteData(rowData.getBeforeColumnsList());
                    }
                } else {
                    System.out.println("-------> before");
                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> after");
                    printColumn(rowData.getAfterColumnsList());
                }
            }

        }
    }


    private void printColumn(@NotNull List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            if (column.getSqlType() == Types.BLOB || column.getSqlType() == Types.CLOB) {
                LOGGER.info("colType:{} colMysqlTYpe:{}", column.getSqlType(), column.getMysqlType());
            } else {
                LOGGER.info("{} : {}  update={} ", column.getName(), column.getValue(), column.getUpdated());
            }
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
