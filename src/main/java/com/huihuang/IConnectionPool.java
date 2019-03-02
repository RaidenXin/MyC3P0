package com.huihuang;

import java.sql.Connection;

//连接数据库池接口
public interface IConnectionPool {

	// 获取连接(重复利用机制)
	public Connection getConnection();

	// 释放连接(可回收机制)
	public void releaseConnection(Connection connection);
}
