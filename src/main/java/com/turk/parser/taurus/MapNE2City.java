package com.turk.parser.taurus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.turk.db.GPCommonDB;
import com.turk.util.LogMgr;

/**
 * 联通数据通过网元查找地市
 * @author Administrator
 *
 */
public class MapNE2City {

	private static MapNE2City _instance = null;
	
	private static Lock loadLock = new ReentrantLock();
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Map<Integer,Integer> 
		_NECityMap = new HashMap<Integer, Integer>();
	
	public static MapNE2City getInstance()
	{	
		if(_instance == null)
		{//第一个进来的线程先加载，其他线程等待
			loadLock.lock();
			try
			{
				if(_instance == null)
					_instance = new MapNE2City();
			}catch (Exception e) {
				log.error("网格初始化类异常:" + e.getMessage() ,e);
			}
			finally
			{
				loadLock.unlock();
				//return null;
			}
		}
		return _instance;
	}
	
	
	public MapNE2City()
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			//log.debug("Starting getConnection...");
			conn = GPCommonDB.getConnection();
			//log.debug("GetConnection done...");
			if (conn == null)
			{
				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = "";
			strSql = "SELECT NE_ID,CITY_ID FROM CFG_MAP_NE2CITY";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		if(!_NECityMap.containsKey(rs.getInt("NE_ID")))
	    		{
	    			_NECityMap.put(rs.getInt("NE_ID"), rs.getInt("CITY_ID"));
	    		}
	    	}
	    	
		}catch (Exception e) {
				
				log.error("MapNE2CITY ERROR:" + e.getMessage() ,e);
		}
		finally
		{
			GPCommonDB.close(rs, pstmt, conn);
		}
	}
	
	public Integer getCityID(int neid)
	{
		if(_NECityMap.get(neid)==null)
		{
			return 531;
		}
		return _NECityMap.get(neid);
	}
	
	public void Clear()
	{
		this._NECityMap.clear();
		_instance = null;
	}
}
