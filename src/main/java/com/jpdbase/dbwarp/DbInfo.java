package com.jpdbase.dbwarp;

public class DbInfo {

    private String dbName;
    private String dbHost;
    private int dbPort;
    private String userName;
    private String userPwd;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getDbHost() {
        return dbHost;
    }

    public void setDbHost(String dbHost) {
        this.dbHost = dbHost;
    }

    public int getDbPort() {
        return dbPort;
    }

    public void setDbPort(int dbPort) {
        this.dbPort = dbPort;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getUserPwd() {
        return userPwd;
    }

    public void setUserPwd(String userPwd) {
        this.userPwd = userPwd;
    }


    public DbInfo(String dbName, String dbHost, int dbPort, String userName, String userPwd) {
        this.dbName = dbName;
        this.dbHost = dbHost;
        this.dbPort = dbPort;
        this.userName = userName;
        this.userPwd = userPwd;
    }

    @Override
    public String toString() {
        return String.format("dbHost:%s, dbPort:%d , dbName:%s, dbUser:%s", dbHost, dbPort, dbName, userName);
    }

    public DbInfo() {
    }
}
