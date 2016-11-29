package com.turk.db;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.util.LogMgr;

/**
 * ��ʱΪ����GP���ݿⴴ��ʹ��
 * @author Administrator
 *
 */
public class GPDBPool {

	static DataSource dataSource = null;

	static BasicDataSource basicDS = null;

	private static Logger log = LogMgr.getInstance().getSystemLogger();

	/**
	 * �ر�����
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
			log.error("GPDBPool: ���Թر�ԭ�е����ݿ����ӳ� [" + 
					dataSource.getClass().getName() + "]ʱʧ��.", e);
		}
		finally
		{
			dataSource = null;
		}
	}

	/**
	 * ��������
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
	 * ��ӡ���ӳ���Ϣ
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
			log.info(String.format("���ӳ���Ϣ:�����(��ǰ/���)=%s/%s,��������(��ǰ/���)=%s/%s", new Object[] { Integer.valueOf(active), Integer.valueOf(maxActive), Integer.valueOf(idle), Integer.valueOf(maxIdle) }));
		}
	}

	/**
	 * �������ӳ�����
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
			log.debug("DbPool: �������ݿ����ӳأ�" + name);
		}
		catch (Exception e)
		{
			log.error("DbPool: ��������Դ " + name + " ʧ�ܣ�", e);
		}
		return dataSource;
	}
	
	public static void main(String[] args)
	{
		
	}
	
}