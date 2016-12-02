package com.turk.parser.taurus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.log4j.Logger;
import com.turk.db.GPCommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class MapImsiMsisdn {
	
	private static MapImsiMsisdn _instance = null;
	
	//private static Lock loadLock = new ReentrantLock();
	
	private static ReadWriteLock lock = new ReentrantReadWriteLock(); 
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Map<Long,Long> _imsiMap = new HashMap<Long, Long>();
	
	public boolean Loading = false;
	
	public static MapImsiMsisdn getInstance()
	{
		
			if(_instance == null)
			{//��һ���������߳��ȼ��أ������̵߳ȴ�
				Lock writeLock = lock.writeLock();
				writeLock.lock();  
				try
				{
					if(_instance == null)
						_instance = new MapImsiMsisdn();
				}catch (Exception e) {
					log.error("�����ʼ�����쳣:" + e.getMessage() ,e);
				}
				finally
				{
					writeLock.unlock();  
					//return null;
				}
			}
			return _instance;
		
	}
	
	public MapImsiMsisdn()
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
				log.error("��������ж�ȡ��Ϣʧ��,ԭ��:�޷���ȡ���ݿ�����.");
				Thread.sleep(60*1000L);
				return;
			}
			
			log.debug("Loading imsi/msisidn....");
			Loading = true;
			String strSql = "";
			//��ȡԤ�������
			strSql = "SELECT IMSI,MSISDN FROM CFG_MAP_IMSI_MSISDN";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		try
	    		{
		    		if(!_imsiMap.containsKey(rs.getLong("IMSI")))
		    		{
		    			if(rs.getString("MSISDN") != null)
		    			{
		    				String msisdn = rs.getString("MSISDN");
		    				String strmsisdn = Util.findByRegex(msisdn, "[0-9]*", 0);
		    				if(strmsisdn != null)
		    				{
		    					if(!strmsisdn.isEmpty())
		    					{
		    						_imsiMap.put(rs.getLong("IMSI"), 
				    					Long.parseLong(strmsisdn));
		    					}
		    				}
		    			}
			    	}
	    		}catch (Exception e) {
					
					log.error("Init imsi/msisidn error:" + e.getMessage() ,e);
	    		}
	    	}
	    	
	    	log.debug("Load imsi/msisidn finish!");
	    	Loading = false;
		}catch (Exception e) {
				
				log.error("Init imsi/msisidn error:" + e.getMessage() ,e);
		}
		finally
		{
			GPCommonDB.close(rs, pstmt, conn);
			
		}
	}
	
	public Long getMSISDN(Long imsi)
	{
		//Lock readLock = lock.readLock();  
		try {  
			Long msisdn = _imsiMap.get(imsi);
			if(msisdn == null)
				return 0L;
			return msisdn;
		} finally {  
            // �ͷ�readLock  
            //readLock.unlock();  
        }  
	}
	
	public void Clear()
	{
		_imsiMap.clear();
		_instance = null;
	}
}
