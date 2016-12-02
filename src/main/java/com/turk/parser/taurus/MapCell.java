package com.turk.parser.taurus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;

public class MapCell {
	private static MapCell _instance = null;
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Map<Long,String> _cellMap = new HashMap<Long, String>();
	
	
	
	public static MapCell getInstance()
	{
		if(_instance == null)
			_instance = new MapCell();
		return _instance;
	}
	
	public MapCell()
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
				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = "";
			//获取预定义分组
			strSql = "SELECT CELLID,CELLNAME FROM CFG_MAP_CELLID ";
			pstmt = conn.prepareStatement(strSql);
		    
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		if(!_cellMap.containsKey(rs.getLong("CELLID")))
	    		{
	    			_cellMap.put(rs.getLong("CELLID"), rs.getString("CELLNAME"));
	    		}
	    	}
	    	
		}catch (Exception e) {
				
				log.error("网格初始化类异常:" + e.getMessage() ,e);
		}
		finally
		{
			CommonDB.close(rs, pstmt, conn);
		}
	}
	
	/**
	 * 根据CI获取小区基站名称
	 * @param ci
	 * @return
	 */
	public String getCellName(Long ci)
	{
		String cellname = _cellMap.get(ci);
		if(cellname == null)
			return "";
		return cellname;
	}
}
