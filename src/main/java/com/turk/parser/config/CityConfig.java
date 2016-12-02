package com.turk.parser.config;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;


public class CityConfig {
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private static Lock loadLock = new ReentrantLock();
	
	private static CityConfig _instance = null;
	
	private Map<String,Integer> _cityMap = new HashMap<String, Integer>();
	
	public static CityConfig getInstance()
	{
		if(_instance == null)
		{
			loadLock.lock();
			try
			{
				if(_instance == null)
					_instance = new CityConfig();
			}catch (Exception e) {
				log.error("�����ʼ�����쳣:" + e.getMessage() ,e);
			}
			finally
			{
				loadLock.unlock();
				//return null;
			}
		}
		return _instance;
	}
	
	public CityConfig()
	{
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
				log.error("��������ж�ȡ��Ϣʧ��,ԭ��:�޷���ȡ���ݿ�����.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = "";
			//��ȡԤ�������
			strSql = "SELECT CITY_ID,CITY_NAME,ENNAME FROM CFG_CITY";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		if(!_cityMap.containsKey(rs.getString("ENNAME")))
	    		{
	    			_cityMap.put(rs.getString("ENNAME"), rs.getInt("CITY_ID"));
	    		}
	    	}
		}
		catch (Exception e) {
			
			log.error("�����ʼ�����쳣:" + e.getMessage() ,e);
		}
		finally
		{
			CommonDB.close(rs, pstmt, conn);
		}
		log.debug("load city config sucess!");
	}
	
	public Map<String,Integer> getCityConfigMap()
	{
		return _cityMap;
	}
	
	public int getCityIDbyEnname(String enname)
	{
		int cityid = 0;
		cityid = _cityMap.get(enname);
		return cityid;
	}
	
}
