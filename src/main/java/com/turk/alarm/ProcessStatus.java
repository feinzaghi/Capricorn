package com.turk.alarm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;

/**
 * 程序状态上报
 * @author Administrator
 *
 */
public class ProcessStatus 
	implements Runnable{
	
	private Thread thread = new Thread(this, toString());

	private boolean stopFlag = false;
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private String Status_SQL_Insert 
		= "INSERT INTO ETL_MOD_SYSALARM_LOG  " +
				"(ALARMTIME,ALARMTYPE,ALARMMSG) " +
				" VALUES(SYSDATE,4,'%s')";
	
	private String Status_SQL_Update 
	= "UPDATE ETL_MOD_SYSALARM_LOG  " +
			"SET ALARMTIME = SYSDATE " +
			" WHERE ALARMMSG = '%s' AND ALARMTYPE = 4";
	
	private static ProcessStatus _instance = null;
	
	public static synchronized ProcessStatus getInstance()
	{
		if(_instance == null)
			_instance = new ProcessStatus();
		return _instance;
	}
	
	/**
	 * 上报进程状态
	 */
	public void ReportMasterStatus()
	{
		//String sql = String.format(this.Status_SQL, Util.getHostName());
		String sql = String.format("SELECT COUNT(*) AS NUM FROM ETL_MOD_SYSALARM_LOG "
				+ "WHERE ALARMMSG = '%s' AND ALARMTYPE = 4", Util.getHostName());
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			//log.debug("Starting getConnection...");
			conn = CommonDB.getConnection();
			//log.debug("GetConnection done...");
			if (conn == null)
			{
				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
				Thread.sleep(60*1000L);
				return;
			}
			
			pstmt = conn.prepareStatement(sql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		if(rs.getInt("NUM") > 0)
	    		{
	    			String strExecSQL = String.format(this.Status_SQL_Update, Util.getHostName());
	    			CommonDB.executeUpdate(strExecSQL);
	    		}
	    		else
	    		{
	    			String strExecSQL = String.format(this.Status_SQL_Insert, Util.getHostName());
	    			CommonDB.executeUpdate(strExecSQL);
	    		}
	    	}
	    	
		}catch (Exception e) {
				
				log.error("MapNE2CITY ERROR:" + e.getMessage() ,e);
		}
		finally
		{
			CommonDB.close(rs, pstmt, conn);
		}
		
		
		try {
			CommonDB.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			log.error("上报主节点状态时，记录写入数据库异常",e);
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(!stopFlag)
		{
			try {
				
				ReportMasterStatus();
			
				Thread.sleep(60*1000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	public void stopScan()
	{
	    this.stopFlag = true;
	}

	public void start()
	{
		this.thread.start();
	}
}
