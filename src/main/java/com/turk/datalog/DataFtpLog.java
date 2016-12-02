package com.turk.datalog;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.util.DbPool;
import com.turk.util.LogMgr;

public class DataFtpLog {
	
	private Logger errorlog = LogMgr.getInstance().getErrorLogger();
	
	private static long _time = 0L;
	private static DataFtpLog _instance = null;
	private ArrayList<String> _listLog = new ArrayList<String>();
	
	public static DataFtpLog getInstance(Date collectTime)
	{
		if(_instance == null && _time != collectTime.getTime())
		{
			_instance = new DataFtpLog(collectTime);
			_time = collectTime.getTime();
		}
		return _instance;
	}
	
	public DataFtpLog(Date collectTime)
	{
		Connection con = DbPool.getConn();
		PreparedStatement pstmt = null;
  		ResultSet rs = null;
  		
		try
		{
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String sCollectTime = df.format(collectTime);
			
			String strSQL = String.format("select FILENAME from utl_data_ftpfilelog " +
					"where stamptime = to_date('%s','yyyy-mm-dd hh24:mi:ss')", sCollectTime);
			
			pstmt = con.prepareStatement(strSQL);
  			rs = pstmt.executeQuery();
  			while(rs.next())
  			{
  				if(!_listLog.contains(rs.getString("FILENAME")))
  				{
  					_listLog.add(rs.getString("FILENAME"));
  				}
  			}
			
		}
		catch (Exception e)
		{
			this.errorlog.error("加载ftp采集文件日志异常", e);
			return;
		}
		finally
		{
			try
			{
				if(rs != null)
				{
					rs.close();
				}
				if (pstmt != null)
				{
					pstmt.close();
				}
				if (con != null)
				{
					con.close();
				}
			}
			catch (Exception localException2)
			{
			}
		}
	}
	
	/**
	 * 是否已采集
	 * @param ftppath
	 * @return
	 */
	public boolean IsCollected(String ftppath)
	{
		if(_listLog.contains(ftppath))
			return true;
		else
			return false;
	}
	
}
