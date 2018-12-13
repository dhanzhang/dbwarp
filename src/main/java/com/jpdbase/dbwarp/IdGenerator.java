package com.jpdbase.dbwarp;

import java.util.Date;
import java.util.Random;

public class IdGenerator {

    private final static long beginTs = 1483200000000L;

    private long lastTs = 0L;

    private long processId;
    private int processIdBits = 10;

    private long sequence = 0L;
    private int sequenceBits = 12;

    public static Date TslUpdateTime=null;

    public IdGenerator(int seed) {
        this.processId    =  seed;

    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public synchronized long nextId() {
        long ts = timeGen();
        if (ts < lastTs) {
            // 刚刚生成的时间戳比上次的时间戳还小，出错
            throw new RuntimeException("时间戳顺序错误");
        }
        if (ts == lastTs) {
            // 刚刚生成的时间戳跟上次的时间戳一样，则需要生成一个sequence序列号
            // sequence循环自增
            sequence = (sequence + 1) & ((1 << sequenceBits) - 1);
            // 如果sequence=0则需要重新生成时间戳
            if (sequence == 0) {
                // 且必须保证时间戳序列往后
                ts = nextTs(lastTs);
            }
        } else {
            // 如果ts>lastTs，时间戳序列已经不同了，此时可以不必生成sequence了，直接取0
            sequence = 0L;
        }
        lastTs = ts;
        // 更新lastTs时间戳
        return ((ts - beginTs) << (processIdBits + sequenceBits)) | (processId << sequenceBits) | sequence;
    }

    protected long nextTs(long lastTs) {
        long ts = timeGen();
        while (ts <= lastTs) {
            ts = timeGen();
        }
        return ts;
    }

}
