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

public class MapLac2City {

	private static MapLac2City _instance = null;
	
	private static Lock loadLock = new ReentrantLock();
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Map<String,Integer> _LACCityMap = new HashMap<String, Integer>();
	
	public static MapLac2City getInstance()
	{	
		if(_instance == null)
		{//第一个进来的线程先加载，其他线程等待
			loadLock.lock();
			try
			{
				if(_instance == null)
					_instance = new MapLac2City();
			}catch (Exception e) {
				log.error("MapLac2City init error:" + e.getMessage() ,e);
			}
			finally
			{
				loadLock.unlock();
				//return null;
			}
		}
		return _instance;
	}
	
	
	public MapLac2City()
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
			strSql = "select NET_ID,LAC,CITY_ID,CITY_NAME from cfg_map_lac2city";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		String key = rs.getInt("NET_ID") + "_" + rs.getInt("LAC");
	    		if(!_LACCityMap.containsKey(key))
	    		{
	    			_LACCityMap.put(key, rs.getInt("CITY_ID"));
	    		}
	    	}
	    	
		}catch (Exception e) {
				
			log.error("MapLac2City ERROR:" + e.getMessage() ,e);
		}
		finally
		{
			GPCommonDB.close(rs, pstmt, conn);
		}
	}
	
	public Integer getCityID(int netid,int lac )
	{
		String key = netid + "_" + lac;
		if(_LACCityMap.get(key)==null)
		{
			return 531;
		}
		return _LACCityMap.get(key);
	}
	
	public void Clear()
	{
		this._LACCityMap.clear();
		_instance = null;
	}
}
