package com.huihuang.message;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DBMessage {

    /* 链接属性 */
    private String driverName;
    private String url;
    private String userName;
    private String password;
    private String poolName = "thread01";// 连接池名字
    private int minConnections = 1; // 空闲池，最小连接数
    private int maxConnections = 10; // 空闲池，最大连接数
    private int initConnections = 5;// 初始化连接数
    private long connTimeOut = 1000;// 重复获得连接的频率
    private int maxActiveConnections = 100;// 最大允许的连接数，和数据库对应
    private long connectionTimeOut = 1000 * 60 * 20;// 连接超时时间，默认20分钟
    private Properties properties;
    
    public DBMessage(){
        this.properties = new Properties();
        init("jdbc.properties");
    }

    private void init(String location) {
        doLoadConfig(location);
        this.driverName = properties.getProperty("driverName");
        this.url = properties.getProperty("url");
        this.userName = properties.getProperty("userName");
        this.password = properties.getProperty("password");
        if (null != properties.getProperty("poolName")){
            this.poolName = properties.getProperty("poolName");
        }
        if (null != properties.getProperty("minConnections")){
            this.minConnections = Integer.valueOf(properties.getProperty("minConnections"));
        }
        if (null != properties.getProperty("initConnections")){
            this.initConnections = Integer.valueOf(properties.getProperty("initConnections"));
        }
        if (null != properties.getProperty("connTimeOut")){
            this.connTimeOut = Integer.valueOf(properties.getProperty("connTimeOut"));
        }
        if (null != properties.getProperty("maxActiveConnections")){
            this.maxActiveConnections = Integer.valueOf(properties.getProperty("maxActiveConnections"));
        }
        if (null != properties.getProperty("connectionTimeOut")){
            this.connectionTimeOut = Integer.valueOf(properties.getProperty("connectionTimeOut"));
        }
    }

    /**
     * 加载配置文件
     * @param location 配置文件的位置
     */
    private void doLoadConfig(String location) {
        //把web.xml中的contextConfigLocation对应value值的文件加载到流里面
        try (InputStream resourceAsStream = this.getClass().getClassLoader().getResourceAsStream(location);) {
            //用Properties文件加载文件里的内容
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public int getMinConnections() {
        return minConnections;
    }

    public void setMinConnections(int minConnections) {
        this.minConnections = minConnections;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    public int getInitConnections() {
        return initConnections;
    }

    public void setInitConnections(int initConnections) {
        this.initConnections = initConnections;
    }

    public long getConnTimeOut() {
        return connTimeOut;
    }

    public void setConnTimeOut(long connTimeOut) {
        this.connTimeOut = connTimeOut;
    }

    public int getMaxActiveConnections() {
        return maxActiveConnections;
    }

    public void setMaxActiveConnections(int maxActiveConnections) {
        this.maxActiveConnections = maxActiveConnections;
    }

    public long getConnectionTimeOut() {
        return connectionTimeOut;
    }

    public void setConnectionTimeOut(long connectionTimeOut) {
        this.connectionTimeOut = connectionTimeOut;
    }
}
