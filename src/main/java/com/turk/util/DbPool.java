package com.turk.util;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;

import com.turk.config.SystemConfig;

/**
 * ���ݿ����ӳ�
 * @author Administrator
 *
 */
public class DbPool
{
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
			log.error("DbPool: ���Թر�ԭ�е����ݿ����ӳ� [" + 
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
			log.error("DbPool: error when got a connection from DB pool.", e);
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

	private static DataSource createDataSource()
	{
		String name = "";
		try
		{
			Properties p = new Properties();
			SystemConfig cfg = SystemConfig.getInstance();
			p.put("name", cfg.getPoolName());
			p.put("type", cfg.getPoolType());
			p.put("driverClassName", cfg.getDbDriver());
			p.put("url", cfg.getDbUrl());
			p.put("maxActive", String.valueOf(cfg.getPoolMaxActive()));
			p.put("username", cfg.getDbUserName());
			p.put("password", cfg.getDbPassword());
			p.put("maxIdle", String.valueOf(cfg.getPoolMaxIdle()));
			p.put("maxWait", String.valueOf(cfg.getPoolMaxWait()));
			p.put("validationQuery", cfg.getDbValidationQueryString());

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
}