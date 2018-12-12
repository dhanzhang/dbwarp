package com.jpdbase.dbwarp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Launcher {
    public static final Logger LOGGER = LoggerFactory.getLogger(Launcher.class);
    public static void main(String[] args) {
        System.out.println("Started !");
        LOGGER.info("OK");
        DbWrapper.instance().Init();
    }
}
