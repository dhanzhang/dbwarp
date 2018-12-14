package com.jpdbase.dbwarp;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.jpdbase.dbwarp.SendMock.SendData;
import com.jpdbase.dbwarp.SendMock.SendJson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

public class Launcher {
    public static final Logger LOGGER = LoggerFactory.getLogger(Launcher.class);

    public static void main(String[] args) {
        System.out.println("Started !");
        System.out.println("Loading driver...");
        try {
            Class.forName("com.mysql.jdbc.Driver");
            System.out.println("Driver loaded!");
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Cannot find the driver in the classpath!", e);
        }
        LOGGER.info("OK");
        DbWrapper.instance().Init();
        DbInfo mdb = DbWrapper.instance().MasterDb();
        LOGGER.info("master db info is :" + mdb);

        // 第一种是可变大小线程池，按照任务数来分配线程，
        // 第二种是单线程池，相当于FixedThreadPool(1)
        // 第三种是固定大小线程池。
        // 然后运行
//        e.execute(new MyRunnableImpl());
//        ExecutorService e = Executors.newFixedThreadPool(3);
//
//
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("sendData-pool-%d").build();
        ExecutorService pool = new ThreadPoolExecutor(8, 16, 360L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(8), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
        //        CanalConnectors.newSingleConnector(
//                new InetSocketAddress(AddressUtils.getHostIp(), 11111), "example", "", "");
        pool.submit(new Dispatch("127.0.0.1", 11111, "example", "", ""));
//        pool.submit(new Dispatch("127.0.0.1",11111, "example","",""));
//        pool.submit(new Dispatch("127.0.0.1",11111, "example","",""));
        pool.submit(new SendJson(mdb, 2001, 100));
        pool.submit(new SendJson(mdb, 2002, 100));
        pool.submit(new SendData(mdb, 2001, 5000));
        pool.submit(new SendData(mdb, 2002, 5000));
        pool.submit(new SendJson(mdb, 2003, 100));
        pool.submit(new SendJson(mdb, 2004, 100));
        pool.submit(new SendJson(mdb, 2005, 100));
        pool.submit(new SendData(mdb, 2003, 5000));
        pool.submit(new SendData(mdb, 2004, 5000));
        pool.submit(new SendData(mdb, 2005, 5000));
        pool.shutdown();
        LOGGER.info("Complected !");

    }
}
