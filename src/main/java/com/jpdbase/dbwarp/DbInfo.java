package com.jpdbase.dbwarp;


import java.sql.DriverManager;

public class DbInfo {

    private String dbName;
    private String dbHost;
    private int dbPort;
    private String userName;
    private String userPwd;

    public String getInstanceName() {
        return instanceName;
    }

    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    private String instanceName;

    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean master) {
        isMaster = master;
    }

    private boolean isMaster;

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
        return String.format("dbInstance:%s, dbHost:%s, dbPort:%d , dbName:%s, dbUser:%s", getInstanceName(),  getDbHost(), getDbPort(), getDbName(), getUserName());
    }

    public DbInfo() {
    }


    public com.mysql.jdbc.MySQLConnection CreateConnection() throws Exception {

        Class.forName("com.mysql.jdbc.Driver");
        final String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&characterEncoding=UTF-8", getDbHost(), getDbPort(), getDbName());
        return (com.mysql.jdbc.MySQLConnection) DriverManager.getConnection(url, getUserName(), getUserPwd());
    }
}
