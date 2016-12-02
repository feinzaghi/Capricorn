package com.turk.parser.taurus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.turk.parser.taurus.model.CFG_NUM_HOME;

import com.turk.db.GPCommonDB;
import com.turk.util.LogMgr;

/**
 * 用户号码归属地
 * @author Turk
 *
 */
public class NumHome {

	private static Lock loadLock = new ReentrantLock();
	
	private static NumHome _instance = null;
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Map<Integer,CFG_NUM_HOME> 
		_sectionMap = new HashMap<Integer, CFG_NUM_HOME>();
	
	public static NumHome getInstance()
	{
		if(_instance == null)
		{
			loadLock.lock();
			try
			{
				if(_instance == null)
					_instance = new NumHome();
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
	
	public NumHome()
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			conn = GPCommonDB.getConnection();
			if (conn == null)
			{
				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = "";
			//获取预定义分组
			strSql = "SELECT ID,SECTION_NO,CITY_ID,CITY_NAME,CARD_TYPE,PROVINCE,CITY FROM CFG_NUM_HOME";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		if(!_sectionMap.containsKey(rs.getLong("SECTION_NO")))
	    		{
	    			CFG_NUM_HOME obj = new CFG_NUM_HOME();
	    			obj.setID(rs.getInt("ID"));
	    			obj.setSectionNo(rs.getInt("SECTION_NO"));
	    			obj.setCityID(rs.getInt("CITY_ID"));
	    			obj.setCityName(rs.getString("CITY_NAME"));
	    			obj.setCardType(rs.getString("CARD_TYPE"));
	    			obj.setProvince(rs.getString("PROVINCE"));
	    			obj.setCity(rs.getString("CITY"));
	    			
	    			_sectionMap.put(obj.getSectionNo(), obj);
	    		}
	    	}
	    	
		}catch (Exception e) {
				
				log.error("网格初始化类异常:" + e.getMessage() ,e);
		}
		finally
		{
			GPCommonDB.close(rs, pstmt, conn);
		}
	}
	
	/**
	 * 获取用户归属地
	 * @param sectionno
	 * @return
	 */
	public CFG_NUM_HOME getRegion(int sectionno)
	{
		CFG_NUM_HOME region = _sectionMap.get(sectionno);
		return region;
	}
}
