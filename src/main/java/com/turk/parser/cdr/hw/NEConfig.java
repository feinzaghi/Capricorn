package com.turk.parser.cdr.hw;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;

/**
 * 网元配置
 * @author Administrator
 *
 */
public class NEConfig {

	private static Logger log = LogMgr.getInstance().getSystemLogger();
	private static Map<Integer,Map<String,NEInfo>> 
		_nemaps = new HashMap<Integer, Map<String, NEInfo>>();
	private static NEConfig _instance = null;
	
	public static NEConfig getInstance(int cityid) 
	{
		if(_instance == null || !_nemaps.containsKey(cityid))
			_instance = new NEConfig(cityid);
		
		return _instance;
	}
	
	public NEConfig(int cityid)
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			
			Map<String,NEInfo> maps = new HashMap<String, NEInfo>();
			
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
			strSql = "select CITY_ID,NE_BSC_ID,NE_BTS_ID,NE_CELL_ID,NE_CARR_ID," +
					"BSC,BTS,CELL,CARR,LONGITUDE,LATITUDE,CARR_SEQ,LOCALBTS,ANT_AZIMUTH " +
					"from cfg_map_dev_to_ne where city_id = " + cityid;
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		NEInfo info = new NEInfo();
	    		info.setCITY_ID(rs.getInt("CITY_ID"));
	    		info.setNE_BSC_ID(rs.getString("NE_BSC_ID"));
	    		info.setNE_BTS_ID(rs.getString("NE_BTS_ID"));
	    		info.setNE_CELL_ID(rs.getString("NE_CELL_ID"));
	    		info.setNE_CARR_ID(rs.getString("NE_CARR_ID"));
	    		info.setBSC_ID(rs.getInt("BSC"));
	    		info.setBTS_ID(rs.getInt("BTS"));
	    		info.setCELL_ID(rs.getInt("LOCALBTS"));
	    		info.setSECTOR_ID(rs.getInt("CELL"));
	    		info.setCARR_ID(rs.getInt("CARR"));
	    		info.setLongitude(rs.getDouble("LONGITUDE"));
	    		info.setLatitude(rs.getDouble("LATITUDE"));
	    		info.setANT_AZIMUTH(rs.getInt("ANT_AZIMUTH"));
	    		String key = info.getBSC_ID() + "_" 
	    					+ info.getCELL_ID() + "_"
	    					+ info.getSECTOR_ID() + "_"
	    					+ info.getCARR_ID();
	    		maps.put(key, info);
	    	}
	    	_nemaps.put(cityid, maps);
		}catch (Exception e) {
				
				log.error("网格初始化类异常:" + e.getMessage() ,e);
		}
		finally
		{
			CommonDB.close(rs, pstmt, conn);
		}
	}
	
	public NEInfo getNEInfo(int cityid,String nekey)
	{
		if(_nemaps.containsKey(cityid))
		{
			Map<String,NEInfo> maps = _nemaps.get(cityid);
			if(maps.containsKey(nekey))
			{
				return maps.get(nekey);
			}
			else
			{
				return null;
			}
		}
		else
		{
			return null;
		}
	}
}
