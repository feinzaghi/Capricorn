package com.turk.parser.taurus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.turk.parser.taurus.model.MOD_CELL;
import com.turk.db.GPCommonDB;
import com.turk.util.LogMgr;

public class MapModCell {
	
	private static Lock loadLock = new ReentrantLock();
	
	private static MapModCell _instance = null;
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Map<Long,MOD_CELL> _cellMap = new HashMap<Long, MOD_CELL>();
	
	private Map<String,MOD_CELL> _cellMapUnicom = new HashMap<String, MOD_CELL>();
	
	private Map<String,MOD_CELL> _cellMapOper = new HashMap<String, MOD_CELL>();
	
	private Map<String,MOD_CELL> _btsMap = new HashMap<String, MOD_CELL>();
	
	private Map<String,MOD_CELL> _cellMapTelecom = new HashMap<String, MOD_CELL>();
	
	public boolean Loading = false;
	
	public static MapModCell getInstance()
	{
		if(_instance == null)
		{
			loadLock.lock();
			try
			{
				if(_instance == null)
					_instance = new MapModCell();
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
	
	
	public MapModCell()
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
			
			Loading = true;
			String strSql = "";
			//获取预定义分组
			strSql = "SELECT OPERATOR,IPINT,CELL_SYS_ID,CELL_NAME,BTS_NAME,LAC,CI,CITY_ID,LONG,LAT " +
					" FROM MOD_CELL";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		long ipint = rs.getLong("IPINT");
	    		long cellsysid = rs.getLong("CELL_SYS_ID");
	    		String btsname = rs.getString("BTS_NAME");
	    		String cellname = rs.getString("CELL_NAME");
	    		int cityid = rs.getInt("CITY_ID");
	    		double lon = rs.getDouble("LONG");
	    		double lat = rs.getDouble("LAT");
	    		int operator = rs.getInt("OPERATOR");
	    		int lac = rs.getInt("LAC");
	    		int ci = rs.getInt("CI");
	    		
	    		if(rs.getLong("IPINT")!=0 && !_cellMap.containsKey(rs.getLong("IPINT")))
	    		{
	    			MOD_CELL model = new MOD_CELL();
	    			model.setIPINT(ipint);
	    			model.setCellSysID(cellsysid);
	    			model.setBtsName(btsname);
	    			model.setCellName(cellname);
	    			model.setLon(lon);
	    			model.setLat(lat);
	    			model.setLAC(lac);
	    			model.setCI(ci);
	    			model.setCityID(cityid);
	    			_cellMap.put(ipint, model);
	    		}
	    		
	    		
	    		
	    		String sLac_CI = operator + "_" + lac + "_" + ci;
	    		if(!_cellMapUnicom.containsKey(sLac_CI))
	    		{
	    			MOD_CELL model = new MOD_CELL();
	    			model.setIPINT(ipint);
	    			model.setCellSysID(cellsysid);
	    			model.setBtsName(btsname);
	    			model.setCellName(cellname);
	    			model.setLon(lon);
	    			model.setLat(lat);
	    			model.setLAC(lac);
	    			model.setCI(ci);
	    			_cellMapUnicom.put(sLac_CI, model);
	    		}
	    		
	    		if(!_btsMap.containsKey(btsname))
	    		{
	    			MOD_CELL model = new MOD_CELL();
	    			model.setCellSysID(cellsysid);
	    			model.setBtsName(btsname);
	    			model.setCellName(cellname);
	    			model.setCityID(cityid);
	    			model.setLon(lon);
	    			model.setLat(lat);
	    			model.setLAC(lac);
	    			model.setCI(ci);
	    			_btsMap.put(btsname, model);
	    		}
	    		
	    		String operKey = operator + "_" + btsname;
	    		if(!_cellMapOper.containsKey(operKey))
	    		{
	    			MOD_CELL model = new MOD_CELL();
	    			model.setCellSysID(cellsysid);
	    			model.setBtsName(btsname);
	    			model.setCellName(cellname);
	    			model.setCityID(cityid);
	    			model.setLon(lon);
	    			model.setLat(lat);
	    			model.setLAC(lac);
	    			model.setCI(ci);
	    			_cellMapOper.put(operKey, model);
	    		}
	    		
	    		if(operator == 3 && lac < 100)
	    		{
	    			if(!_cellMapTelecom.containsKey(String.valueOf(ci)))
		    		{
		    			MOD_CELL model = new MOD_CELL();
		    			model.setCellSysID(cellsysid);
		    			model.setBtsName(btsname);
		    			model.setCellName(cellname);
		    			model.setCityID(cityid);
		    			model.setLon(lon);
		    			model.setLat(lat);
		    			model.setLAC(lac);
		    			model.setCI(ci);
		    			_cellMapTelecom.put(String.valueOf(ci), model);
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
		Loading = false;
		log.debug("load cell bts sucess!");
	}
	
	public MOD_CELL getCellInfo(long ipint)
	{
		MOD_CELL model = _cellMap.get(ipint);
		return model;
	}
	
	public MOD_CELL getCellInfo(String operlacci)
	{
		MOD_CELL model = _cellMapUnicom.get(operlacci);
		return model;
	}
	
	public MOD_CELL getTelecomCellInfo(String ci)
	{
		if(ci==null)
			return null;
		MOD_CELL model = _cellMapTelecom.get(ci);
		return model;
	}
	
	
	public MOD_CELL getCellInfoByCTJSTID(String CTJSTID)
	{
		MOD_CELL model = _btsMap.get(CTJSTID);
		return model;
	}
	
	public MOD_CELL getCellInfoByCTJSTIDANDOper(int nOper,String CTJSTID)
	{
		String operKey = nOper + "_" + CTJSTID;
		MOD_CELL model = _cellMapOper.get(operKey);
		return model;
	}
	
	public void Clear()
	{
		this._cellMap.clear();
		this._cellMapUnicom.clear();
		_instance = null;
	}
}
