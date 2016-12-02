package com.turk.parser.taurus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.turk.parser.taurus.model.MOD_MONITOR_MOBILE;

import com.turk.db.GPCommonDB;
import com.turk.util.LogMgr;

/**
 * 触网监控配置
 * @author Administrator
 *
 */
public class MonitorMobileConfig{
	
	private static Lock loadLock = new ReentrantLock();
	
	private static boolean isClear = false;
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private static MonitorMobileConfig _instance = null;
	
	/**
	 * 触网用户监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _touchMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	/**
	 * 开机监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _powerupMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	/**
	 * 特定联系人监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _callnumalarmMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	/**
	 * 特定区域监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _exitareaMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	/**
	 * 特定区域监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _enterareaMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	/**
	 * 特定LAC区域监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _exitLACMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	/**
	 * 特定LAC区域监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _enterLACMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	/**
	 * 归属地区域号码监控
	 */
	private Map<String,ArrayList<MOD_MONITOR_MOBILE>> _belongLACAreaMap 
		= new HashMap<String, ArrayList<MOD_MONITOR_MOBILE>>();
	
	
	public static void main(String[] args)
	{
		MonitorMobileConfig.getInstance();
	}
	
	
	public synchronized static MonitorMobileConfig getInstance()
	{

		
		if(_instance == null)
		{
			//loadLock.lock();
			try
			{
				if(_instance == null)
					_instance = new MonitorMobileConfig();
			}catch (Exception e) {
				log.error("初始化用户监控配置异常:" + e.getMessage() ,e);
			}
			finally
			{
				//loadLock.unlock();
				//return null;
			}
		}
		return _instance;
		
	}
	
	public MonitorMobileConfig()
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
			//获取预定义分组
			strSql = "SELECT MONITOR_ID,CASE_ID,MONITOR_NAME,START_TIME,END_TIME," +
					"NUM_TYPE,NUM_LIST,ALARM_LEVEL,A_TOUCH_NET,A_POWER_UP," +
					"A_CALL_NUM_ALARM,CALL_NUM_LIST,A_EXIT_AREA,EXIT_AREA_CELL_LIST," +
					"A_ENTER_AREA,ENTER_AREA_CELL_LIST,EXCLUDE_NUM_LIST," +
					"A_ENTER_LAC,ENTER_LAC_LIST,A_EXIT_LAC,EXIT_LAC_LIST,A_AREACODE,AREACODE_LIST," +
					"USER_NAME,SEND_MSG_LIST " +
					"FROM MOD_MONITOR_MOBILE WHERE set_disable = 'f' and start_time < now() and end_time > now()";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		MOD_MONITOR_MOBILE mobile = new MOD_MONITOR_MOBILE();
	    		mobile.setMonitorID(rs.getInt("MONITOR_ID"));
	    		mobile.setCaseID(rs.getString("CASE_ID"));
    			mobile.setMonitorName(rs.getString("MONITOR_NAME"));
    			mobile.setStartTime(rs.getTimestamp("START_TIME"));
    			mobile.setEndTime(rs.getTimestamp("END_TIME"));
    			mobile.setNumType(rs.getString("NUM_TYPE"));
    			mobile.setNumList(rs.getString("NUM_LIST"));
    			mobile.setAalarmLevel(rs.getString("ALARM_LEVEL"));
    			mobile.setCallNumList(rs.getString("CALL_NUM_LIST"));
    			mobile.setExitAreaCellList(rs.getString("EXIT_AREA_CELL_LIST"));
    			mobile.setEnterAreaCellList(rs.getString("ENTER_AREA_CELL_LIST"));
    			
    			HashMap<String,String> mapExcludeNum = new HashMap<String, String>();
    			if(rs.getString("EXCLUDE_NUM_LIST")!=null)
    			{
    				String[] sExcludeNums = rs.getString("EXCLUDE_NUM_LIST").split(",",-1);
    				for(String num : sExcludeNums)
    				{
    					if(num.trim().isEmpty())
    						continue;
    					
    					if(!mapExcludeNum.containsKey(num))
    					{
    						mapExcludeNum.put(num, num);
    					}
    				}
    			}
    			mobile.setExcludeNumList(mapExcludeNum);
    			
    			
    			HashMap<Integer,String> mapEnterLAC = new HashMap<Integer, String>();
    			if(rs.getString("ENTER_LAC_LIST")!=null)
    			{
	    			String[] sEnterLACs = rs.getString("ENTER_LAC_LIST").split(",",-1);
	    			for(String lac:sEnterLACs)
	    			{
	    				int nLac = -1;
	    				if(!lac.isEmpty())
	    					nLac = Integer.parseInt(lac);
	    				if(!mapEnterLAC.containsKey(nLac))
	    				{
	    					mapEnterLAC.put(nLac, lac);
	    				}
	    			}
    			}
    			mobile.setEnterLACList(mapEnterLAC);
    			
    			HashMap<Integer,String> mapExitLAC = new HashMap<Integer, String>();
    			if(rs.getString("EXIT_LAC_LIST")!=null)
    			{
	    			String[] sExitLACs = rs.getString("EXIT_LAC_LIST").split(",",-1);
	    			for(String lac:sExitLACs)
	    			{
	    				int nLac = -1;
	    				if(!lac.isEmpty())
	    					nLac = Integer.parseInt(lac);
	    				if(!mapExitLAC.containsKey(nLac))
	    				{
	    					mapExitLAC.put(nLac, lac);
	    				}
	    			}
    			}
    			mobile.setExitLACList(mapExitLAC);
    			
    			
    			mobile.setUserName(rs.getString("USER_NAME"));
    			mobile.setSendMsgList(rs.getString("SEND_MSG_LIST"));
    			
    			String[] numlist = new String[]{};
    			if(rs.getString("NUM_LIST") != null && !rs.getString("NUM_LIST").trim().isEmpty())
    				numlist = rs.getString("NUM_LIST").trim().split(",",-1);
    			
    			for(String usernum:numlist)
    			{
	    			if(usernum.trim().isEmpty())
	    				continue;
	    			
		    		if(rs.getBoolean("A_TOUCH_NET"))
		    		{
		    			if(!_touchMap.containsKey(usernum))
			    		{
		    				ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
			    			_touchMap.put(usernum, list);
			    			log.debug("加入触网用户:" + usernum);
			    		}
		    			else
		    			{
		    				ArrayList<MOD_MONITOR_MOBILE> list = _touchMap.get(usernum);
		    				if(list!=null)
		    				{
		    					list.add(mobile);
		    					log.debug("加入触网用户:" + usernum);
		    				}
		    			}
		    		}
		    		
		    		if(rs.getBoolean("A_POWER_UP"))
		    		{
		    			if(!_powerupMap.containsKey(usernum))
			    		{
		    				ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
		    				_powerupMap.put(usernum, list);
		    				log.debug("加入开关机用户:" + usernum);
			    		}
		    			else
		    			{
		    				ArrayList<MOD_MONITOR_MOBILE> list = _powerupMap.get(usernum);
		    				list.add(mobile);
		    				log.debug("加入开关机用户:" + usernum);
		    			}
		    		}
		    		
		    		if(rs.getBoolean("A_CALL_NUM_ALARM"))
		    		{
		    			if(!_callnumalarmMap.containsKey(usernum))
			    		{
		    				ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
		    				_callnumalarmMap.put(usernum, list);
		    				log.debug("加入特定联系用户:" + usernum);
			    		}
		    			else
		    			{
		    				ArrayList<MOD_MONITOR_MOBILE> list = _callnumalarmMap.get(usernum);
		    				list.add(mobile);
		    				log.debug("加入开关机用户:" + usernum);
		    			}
		    		}
		    		
		    		if(rs.getBoolean("A_EXIT_AREA"))
		    		{
		    			if(!_exitareaMap.containsKey(usernum))
			    		{
		    				ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
		    				_exitareaMap.put(usernum, list);
		    				log.debug("加入离开范围用户:" + usernum);
			    		}
		    			else
		    			{
		    				ArrayList<MOD_MONITOR_MOBILE> list = _exitareaMap.get(usernum);
		    				list.add(mobile);
		    				log.debug("加入离开范围用户:" + usernum);
		    			}
		    		}
		    		
		    		if(rs.getBoolean("A_ENTER_AREA"))
		    		{
		    			if(!_enterareaMap.containsKey(usernum))
			    		{
		    				ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
		    				_enterareaMap.put(usernum, list);
		    				log.debug("加入进入范围用户:" + usernum);
			    		}
		    			else
		    			{
		    				ArrayList<MOD_MONITOR_MOBILE> list = _enterareaMap.get(usernum);
		    				list.add(mobile);
		    				log.debug("加入进入范围用户:" + usernum);
		    			}
		    		}
		    		
		    		if(rs.getBoolean("A_ENTER_LAC"))
		    		{
		    			if(!_enterLACMap.containsKey(usernum))
			    		{
		    				ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
		    				_enterLACMap.put(usernum, list);
		    				log.debug("加入进入LAC范围用户:" + usernum);
			    		}
		    			else
		    			{
		    				ArrayList<MOD_MONITOR_MOBILE> list = _enterLACMap.get(usernum);
		    				list.add(mobile);
		    				log.debug("加入进入LAC范围用户:" + usernum);
		    			}
		    		}
		    		
		    		if(rs.getBoolean("A_EXIT_LAC"))
		    		{
		    			if(!_exitLACMap.containsKey(usernum))
			    		{
		    				ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
		    				_exitLACMap.put(usernum, list);
		    				log.debug("加入exit LAC范围用户:" + usernum);
			    		}
		    			else
		    			{
		    				ArrayList<MOD_MONITOR_MOBILE> list = _exitLACMap.get(usernum);
		    				list.add(mobile);
		    				log.debug("加入exit LAC范围用户:" + usernum);
		    			}
		    		}
    			}
    			
    			
    			//归属地区域监控
    			if(rs.getBoolean("A_AREACODE"))
    			{
    				String[] citylist = new String[]{};
    				if(rs.getString("AREACODE_LIST")!=null && !rs.getString("AREACODE_LIST").trim().isEmpty())
    					citylist = rs.getString("AREACODE_LIST").split(",",-1);
    				for(String cityid : citylist)
    				{
    					if(cityid.trim().isEmpty())
    						continue;
    					
    					if(!_belongLACAreaMap.containsKey(cityid))
    					{
    						ArrayList<MOD_MONITOR_MOBILE> list = new ArrayList<MOD_MONITOR_MOBILE>();
		    				list.add(mobile);
		    				_belongLACAreaMap.put(cityid, list);
    					}
    					else
    					{
    						ArrayList<MOD_MONITOR_MOBILE> list = _belongLACAreaMap.get(cityid);
		    				list.add(mobile);
    					}
    					log.debug("加入归属地:" + cityid);
    				}
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

	public ArrayList<MOD_MONITOR_MOBILE> getTouchMobile(String numlist)
	{
		return _touchMap.get(numlist);
	}
	
	public ArrayList<MOD_MONITOR_MOBILE> getPowerMobile(String numlist)
	{
		return _powerupMap.get(numlist);
	}
	
	public ArrayList<MOD_MONITOR_MOBILE> getCallNumMobile(String numlist)
	{
		return _callnumalarmMap.get(numlist);
	}
	
	public ArrayList<MOD_MONITOR_MOBILE> getExitAreaMobile(String numlist)
	{
		return _exitareaMap.get(numlist);
	}
	
	public ArrayList<MOD_MONITOR_MOBILE> getEnterAreaMobile(String numlist)
	{
		return _enterareaMap.get(numlist);
	}
	
	public ArrayList<MOD_MONITOR_MOBILE> getExitLACMobile(String numlist)
	{
		return _exitLACMap.get(numlist);
	}
	
	public ArrayList<MOD_MONITOR_MOBILE> getEnterLACMobile(String numlist)
	{
		return _enterLACMap.get(numlist);
	}
	
	public ArrayList<MOD_MONITOR_MOBILE> getBlongLACArea(String cityid)
	{
		return _belongLACAreaMap.get(cityid);
	}
	
	
	public void Clear()
	{
		isClear = true;
		this._touchMap.clear();
		this._powerupMap.clear();
		this._callnumalarmMap.clear();
		this._enterareaMap.clear();
		this._exitareaMap.clear();
		this._exitLACMap.clear();
		this._enterLACMap.clear();
		this._belongLACAreaMap.clear();
		_instance = null;
		isClear = false;
	}
	
}
