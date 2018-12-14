package com.jpdbase.dbwarp;

import com.alibaba.otter.canal.protocol.CanalEntry;

import java.util.List;

/**
 * @author dhz
 * 处理常规的   数据插入， 删除和更新操作
 */
public interface IEventHandler {

    void attachTdDbInstance(DbInfo dbInfo);


    void insertData(List<CanalEntry.Column> columns);

    void deleteData(List<CanalEntry.Column> columns);

    void updateData(List<CanalEntry.Column> beforeColumnsList, List<CanalEntry.Column> afterColumnsList);
}
