package com.turk.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;

/**
 * 采集DB 日志
 * @author Administrator
 *
 */
public final class DBLogger
{
	private static final String INSERT_SQL = "INSERT INTO LOG_CLT_INSERT (OMCID,CLT_TBNAME,STAMPTIME,VSYSDATE,INSERT_COUNTNUM,IS_CAL,TASKID) VALUES (?,UPPER(?),?,sysdate,?,0,?)";
	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	private static final DBLogger INSTANCE = new DBLogger();
	private Connection conn;

	private DBLogger()
	{
		this.conn = DbPool.getConn();
	}

	public static synchronized DBLogger getInstance()
	{
		return INSTANCE;
	}

	public synchronized void log(int deviceID, String tableName, long stampTime, int count, int taskID,double TimeLength)
  	{
		log(deviceID, tableName, new Date(stampTime), count, taskID, TimeLength);
  	}
	
	public synchronized void log(int deviceID, String tableName, long stampTime, 
			int count, int taskID,double TimeLength, double bytes)
  	{
		log(deviceID, tableName, new Date(stampTime), count, taskID, TimeLength, bytes);
  	}

	/**
	 * 
	 * @param deviceID
	 * @param tableName
	 * @param stampTime
	 * @param count
	 * @param taskID
	 * @param TimeLength 入库时长
	 * @param bytes 入库文件大小
	 */
	public synchronized void log(int deviceID, String tableName, String stampTime, 
			int count, int taskID,double TimeLength,double bytes)
	{
		PreparedStatement preparedStatement = null;

		String sql = "";
		try
		{
			if ((this.conn == null) || (this.conn.isClosed()))
			{
				this.conn = DbPool.getConn();
			}
			String ss = stampTime;
			if ((ss != null) && (ss.length() == 13))
				ss = ss + ":00:00";
			if ((ss != null) && (ss.length() == 10))
				ss = ss + " 00:00:00";
			if ((ss != null) && (ss.length() == 8))
			{
				Date time = Util.getDate3(ss);
				ss = Util.getDateString(time);
			}
			if(Util.isOracle())
			{
				sql = String.format("INSERT INTO LOG_CLT_INSERT " +
						"(DEVICEID,CLT_TBNAME,STAMPTIME,VSYSDATE,INSERT_COUNTNUM,IS_CAL,TASKID,TIMELENGTH,BYTES) " +
						"VALUES (%d,UPPER('%s'),TO_DATE('%s','YYYY-MM-DD HH24:MI:SS'),sysdate,%d,0,%d,%f,%f)",
						deviceID,tableName == null ? "" : tableName,ss,count,taskID,TimeLength,bytes);
			}
			else if(Util.isMySQL())
			{
				sql = String.format("INSERT INTO LOG_CLT_INSERT " +
						"(DEVICEID,CLT_TBNAME,STAMPTIME,VSYSDATE,INSERT_COUNTNUM,IS_CAL,TASKID,TIMELENGTH,BYTES) " +
						"VALUES (%d,UPPER('%s'),'%s',sysdate(),%d,0,%d,%f,%f)",
						deviceID,tableName == null ? "" : tableName,ss,count,taskID,TimeLength,bytes);
			}
			preparedStatement = this.conn.prepareStatement(sql);
			preparedStatement.execute();
		}
		catch (Exception e)
		{
			logger.error("记录数据库日志时异常,sql:" + sql, e);
			dispose();
			this.conn = DbPool.getConn();
		}
		finally
		{
			CommonDB.close(null, preparedStatement, null);
		}
	}

	
	public synchronized void log(int deviceID, String tableName, Date stampTime, int count, int taskID,double TimeLength)
	{
		log(deviceID, tableName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(stampTime), count, taskID, TimeLength,0);
	}
	

	
	/**
	 * 
	 * @param deviceID
	 * @param tableName
	 * @param stampTime
	 * @param count
	 * @param taskID
	 * @param TimeLength 入库时长
	 * @param bytes 入库文件大小
	 */
	public synchronized void log(int deviceID, String tableName, Date stampTime, int count, int taskID,double TimeLength,double bytes)
	{
		log(deviceID, tableName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(stampTime), count, taskID, TimeLength,bytes);
	}

	public synchronized void logForHour(int deviceID, String tableName, Date stampTime, int count, int taskID)
	{
		log(deviceID, tableName, new SimpleDateFormat("yyyy-MM-dd HH").format(stampTime), count, taskID,0,0);
	}

	public synchronized void dispose()
	{
		if (this.conn != null)
		{
			try
			{
				this.conn.close();
			}
			catch (Exception localException)
			{
			}
			this.conn = null;
		}
	}
}