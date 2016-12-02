package com.turk.parser.taurus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.parser.taurus.model.CFG_MAP_PN_CELL;

import com.turk.db.GPCommonDB;
import com.turk.util.LogMgr;

public class MapPNCell {
	
	private static MapPNCell _instance = null;
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Map<Integer,ArrayList<CFG_MAP_PN_CELL>> 
		_pnMap = new HashMap<Integer, ArrayList<CFG_MAP_PN_CELL>>();
	
	public static MapPNCell getInstance()
	{
		if(_instance == null)
			_instance = new MapPNCell();
		return _instance;
	}
	
	public MapPNCell()
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
			strSql = "SELECT CELL_NAME,LONGITUDE,LATITUDE,PN,CELL_SYS_ID FROM CFG_MAP_PN_CELL";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		
	    		CFG_MAP_PN_CELL cellinfo = new CFG_MAP_PN_CELL();
    			cellinfo.setCellName(rs.getString("CELL_NAME"));
    			cellinfo.setLongitude(rs.getDouble("LONGITUDE"));
    			cellinfo.setLatitude(rs.getDouble("LATITUDE"));
    			cellinfo.setPN(rs.getInt("PN"));
    			cellinfo.setCellSysID(rs.getLong("CELL_SYS_ID"));
    			
    			
    			
	    		if(!_pnMap.containsKey(rs.getInt("PN")))
	    		{
	    			ArrayList<CFG_MAP_PN_CELL> celllist = new ArrayList<CFG_MAP_PN_CELL>();
	    			celllist.add(cellinfo);
	    			_pnMap.put(rs.getInt("PN"), celllist);
	    		}
	    		else
	    		{
	    			ArrayList<CFG_MAP_PN_CELL> celllist = _pnMap.get(rs.getInt("PN"));
	    			celllist.add(cellinfo);
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
	 * 根据CI获取小区基站名称
	 * @param ci
	 * @return
	 */
	public ArrayList<CFG_MAP_PN_CELL> getCellList(int pn)
	{
		ArrayList<CFG_MAP_PN_CELL> celllist = _pnMap.get(pn);
		
		return celllist;
	}
}
