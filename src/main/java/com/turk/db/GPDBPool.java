package com.turk.db;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;

import com.turk.config.SystemConfig;
import com.turk.util.LogMgr;

/**
 * 临时为连接GP数据库创建使用
 * @author Administrator
 *
 */
public class GPDBPool {

	static DataSource dataSource = null;

	static BasicDataSource basicDS = null;

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	/**
	 * 关闭连接
	 */
	public static void close()
	{
		try
		{
			if (dataSource != null)
			{
				Class classz = dataSource.getClass();
				Class[] types = new Class[0];
				Method method = classz.getDeclaredMethod("close", types);
				if (method != null)
				{
					method.setAccessible(true);
					Object[] args = new Object[0];
					method.invoke(dataSource, args);
				}
			}
		}
		catch (Exception e)
		{
			log.error("GPDBPool: 尝试关闭原有的数据库连接池 [" + 
					dataSource.getClass().getName() + "]时失败.", e);
		}
		finally
		{
			dataSource = null;
		}
	}

	/**
	 * 分配连接
	 * @return
	 */
	public static Connection getConn()
	{
		Connection conn = null;
		try
		{
			if (dataSource == null)
			{
				createDataSource();
			}
			conn = dataSource.getConnection();
		}
		catch (Exception e)
		{
			log.error("GPDBPool: error when got a connection from GP-DB pool.", e);
		}
		return conn;
	}

	/**
	 * 打印连接池信息
	 */
	public static void printPoolInfo()
	{
		if (basicDS == null)
		{
			if (dataSource != null)
			{
				basicDS = (BasicDataSource)dataSource;
			}
		}
		if (basicDS != null)
		{
			int maxActive = basicDS.getMaxActive();
			int maxIdle = basicDS.getMaxIdle();
			int active = basicDS.getNumActive();
			int idle = basicDS.getNumIdle();
			log.info(String.format("连接池信息:活动连接(当前/最大)=%s/%s,空闲连接(当前/最大)=%s/%s", new Object[] { Integer.valueOf(active), Integer.valueOf(maxActive), Integer.valueOf(idle), Integer.valueOf(maxIdle) }));
		}
	}

	/**
	 * 创建连接池配置
	 * @return
	 */
	private static DataSource createDataSource()
	{
		String name = "";
		try
		{
			Properties p = new Properties();
			SystemConfig cfg = SystemConfig.getInstance();
			p.put("name", "GPPOOL");
			p.put("type", cfg.getPoolType());
			p.put("driverClassName", cfg.getGPDbDriver());
			p.put("url", cfg.getGPDbUrl());
			p.put("maxActive", String.valueOf(cfg.getPoolMaxActive()));
			p.put("username", cfg.getGPDbUserName());
			p.put("password", cfg.getGPDbPassword());
			p.put("maxIdle", String.valueOf(cfg.getPoolMaxIdle()));
			p.put("maxWait", String.valueOf(cfg.getPoolMaxWait()));
			p.put("validationQuery", "select now()");

			name = SystemConfig.getInstance().getPoolName();
			dataSource = BasicDataSourceFactory.createDataSource(p);
			log.debug("DbPool: 创建数据库连接池：" + name);
		}
		catch (Exception e)
		{
			log.error("DbPool: 创建数据源 " + name + " 失败：", e);
		}
		return dataSource;
	}
	
	public static void main(String[] args)
	{
		
	}
	
}
