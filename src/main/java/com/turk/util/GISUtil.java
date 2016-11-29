package com.turk.util;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.turk.util.CommonDB;
import com.turk.util.GISUtil;
import com.turk.util.LogMgr;

/**
 * GIS相关应用方法
 */
public class GISUtil {

	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * 构造
	 * 初始化单个地市GIS配置
	 */
	public GISUtil(int CityID)
	{
		this.CityID = CityID;
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		try
		{
			double longitude_l = 0L;
			double latitude_l = 0L;
			double longitude_r = 0L;
			double latitude_r = 0L;
			
			conn = CommonDB.getConnection();
			
			String strSql = String.format("SELECT CITY_ID,CITY_NAME,LONGITUDE_LB," +
					"LATITUDE_LB,LONGITUDE_RB,LATITUDE_RB,LONGITUDE_CENTER," +
					"LATITUDE_CENTER from CFG_CITY WHERE CITY_ID = %d",CityID);
			PreparedStatement prst = conn.prepareStatement(strSql);
			rs = prst.executeQuery();
			while(rs.next())
			{
				longitude_l = rs.getDouble("LONGITUDE_LB");
				latitude_l = rs.getDouble("LATITUDE_LB");
				longitude_r = rs.getDouble("LONGITUDE_RB");
				latitude_r = rs.getDouble("LATITUDE_RB");
			}
			rs.close();
			conn.close();
			if(longitude_l==0||latitude_l==0||longitude_r==0||latitude_r==0)
				return;
			
			//修改为以地图 0，0点为起始原点
			East = longitude_r;
			West = longitude_l;
			South = latitude_l;
			North = latitude_r;
			
			//计算长度
			MaxLong = GetDistance(West,South,East,South);
			MaxWidth = GetDistance(West,South,West,North);
			
			
			
		}
		catch(Exception ex)
		{
			try {
				if(!rs.isClosed())
					rs.close();
				if(!conn.isClosed())
					conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			log.error(ex);
		}
		
	}
	
	public GISUtil()
	{
		
	}
	//private static int _cityID = 0;
	//private static GISUtil _instance;
	//public static GISUtil GetInstance(int CityID)
	//{
	//	if(_instance == null || _cityID != CityID)
	//	{
	//		_instance = new GISUtil(CityID);
	//		_cityID = CityID;
	//	}
	//	return _instance;
	//}
	
	private int _MaxGridM = 0;
	private int _MaxGridN = 0;
	
	/**
	 * 城市长（米）
	 */
	public double MaxLong;
	
	/**
	 * 城市宽（米）
	 */
	public double MaxWidth;
	
	/**
	 * 城市东边经度
	 */
	public double East = 0;
	
	/**
	 * 城市西边经度
	 */
	public double West = 0;
	
	/**
	 * 城市南边维度
	 */
	public double South = 0;
	
	/**
	 * 城市北边维度
	 */
	public double North = 0;
	
	public int CityID = 0;
	
	/**
	 * 获取该地市下最大栅格M值
	 * @param  meter 单格子的边长
	 */
	public int GetMaxGridM(int meter,double lon,double lat)
	{
		//计算最大栅格时需要统计当前纬度的距离
		double curLong = GetDistance(West,lat,East,lat);
		_MaxGridM = (int)(curLong/meter)+1;
		return _MaxGridM;
	}
	
	/**
	 * 获取该地市下最大栅格N值
	 */
	public int GetMaxGridN(int meter)
	{
		_MaxGridN = (int)(MaxWidth/meter)+1;
		return _MaxGridN;
	}
	
	private static double EARTH_RADIUS = 6378137;  
	
	/**
	 * 根据两点经纬度，获取两点之间的距离(米)
	 * @param  lon1 点一经度
	 * @param  lat1 点一维度
	 * @param  lon2 点二经度
	 * @param  lat2 点二维度
	 */
	public static double GetDistance(double lon1,double lat1,double lon2,double lat2)
	{
		
		double Return = 0F;
		double dbfLongDiff;
		double dbfLatDiff;
		double dblTmp;
		double EARTH_SHORT_RADIUS;
		double EARTH_LONG_RADIUS;
		double Tmp;
		double dDistancePow2;
		
		//if(lon1 == 0 || lat1 == 0 || lon2 == 0 || lat2 == 0)
		//    return Return;
		
		 dbfLongDiff = (lon2 - lon1) * Math.PI / 180.0;
		 dbfLatDiff  = (lat2 - lat1) * Math.PI / 180.0;
		 dblTmp	= Math.cos(lat1 * Math.PI / 180);
		 EARTH_SHORT_RADIUS = 40408299981544.355;
		 EARTH_LONG_RADIUS  = 40680631590769.0;
		
		 
		 Tmp = Math.sin(lat1 * Math.PI / 180);

		 Tmp = EARTH_SHORT_RADIUS +
		         (EARTH_LONG_RADIUS - EARTH_SHORT_RADIUS) * Tmp * Tmp;

		 Tmp = EARTH_SHORT_RADIUS * EARTH_LONG_RADIUS / Tmp;
		 
		 dDistancePow2 = Tmp * (dbfLongDiff * dbfLongDiff * dblTmp * dblTmp +
		                   dbfLatDiff * dbfLatDiff);
		 Return = Math.sqrt(dDistancePow2);
		 
		 return Return; 
		 
		 /*
		 double radLat1 = Math.toRadians(lat1);  
		    double radLat2 = Math.toRadians(lat2);  
		    double a = radLat1 - radLat2;  
		    double b = Math.toRadians(lon1) - Math.toRadians(lon2);  
		    double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(Math.abs(a) / 2), 2)  
		        + Math.cos(radLat1) * Math.cos(radLat2)  
		        * Math.pow(Math.sin(Math.abs(b) / 2), 2)));  
		    s = s * EARTH_RADIUS;  
		    s = Math.round(s * 10000) / 10000;  
		    //return s;  
		    
		    
		     
		  double radLat1 = lat1 * Math.PI / 180;
	      double radLat2 = lat2 * Math.PI / 180;
	      double a = radLat1 - radLat2;
	      double b = lon1 * Math.PI / 180 - lon2 * Math.PI / 180;
	      double s = 2 * Math.asin(Math.sqrt(Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1)
	                * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
	      s = s * 6378137.0;// 取WGS84标准参考椭球中的地球长半径(单位:m)
	      s = Math.round(s * 10000) / 10000;

	      return s; 
		 
		 double C = Math.sin(lat1*Math.PI/180)*Math.sin(lat2*Math.PI/180) 
		 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.cos(Math.abs(lon1-lon2)*Math.PI/180);

		 double Distance = EARTH_RADIUS*Math.asin(C)*Math.PI/180;
		 
		 return Distance;*/
	}
	
	/**
	 * 根据点的经纬度，获取该点在当前地市的栅格ID号
	 * M*10000+N 统一坐标系，左下角开始计算
	 * @param  metar 每个栅格的边长
	 * @param  lon 经度
	 * @param  lat 维度
	 */
	public long GetGridID(int metar,double lon,double lat)
	{
		long GridID = 0;
		long M = 0;
		long N = 0;
		
		//根据点的经度，获取点在当前地市全局栅格中的同一维度线上的位置比例乘以最大栅格数，获取当前栅格位置
		M = (long)(((double)(lon - West)/(East - West))*GetMaxGridM(metar,lon,lat)) + 1;
		//M = (long)(GetDistance(West,lat,lon,lat)/100) + 1;
		
		N = (long)(((lat - South)/(North - South))*GetMaxGridN(metar))+1;
		//N = (long)(GetDistance(lon,South,lon,lat)/100) + 1;
		
		GridID = M*10000+N;
		return GridID;
	}
	
	/**
	 * 通过经纬度获取M,N值，通过数组返回，M 0,N 1
	 * @param metar
	 * @param lon
	 * @param lat
	 * @return
	 */
	public List<Integer> GetGridMN(int metar,double lon,double lat)
	{
		List<Integer> MN = new ArrayList<Integer>();
		int M = 0;
		int N = 0;
		
		//根据点的经度，获取点在当前地市全局栅格中的同一维度线上的位置比例乘以最大栅格数，获取当前栅格位置
		M = (int)(((lon-West)/(East - West))*GetMaxGridM(metar,lon,lat)) + 1;
		
		N = (int)(((lat-South)/(North - South))*GetMaxGridN(metar)) + 1;
		
		MN.add(M);
		MN.add(N);
		return MN;
	}
	
	/**
	 * 通过经纬度获取M,N值，通过数组返回，M 0,N 1(默认100m栅格)
	 * @param lon
	 * @param lat
	 * @return
	 */
	public List<Integer> GetGridMN(double lon,double lat)
	{
		List<Integer> MN = new ArrayList<Integer>();
		int M = 0;
		int N = 0;
		
		//根据点的经度，获取点在当前地市全局栅格中的同一维度线上的位置比例乘以最大栅格数，获取当前栅格位置
		M = (int)(((lon-West)/(East - West))*GetMaxGridM(100,lon,lat)) + 1;
		
		N = (int)(((lat-South)/(North - South))*GetMaxGridN(100)) + 1;
		
		MN.add(M);
		MN.add(N);
		return MN;
	}
	
	public int GetGridM(double lon,double lat)
	{
		int GridM = 0;
		//根据点的经度，获取点在当前地市全局栅格中的同一维度线上的位置比例乘以最大栅格数，获取当前栅格位置
		GridM = (int)(((lon-West)/(East - West))*GetMaxGridM(100,lon,lat)) + 1;
		
		return GridM;
	}
	
	public int GetGridN(double lon,double lat)
	{
		int GridN = 0;
		
		//根据点的经度，获取点在当前地市全局栅格中的同一维度线上的位置比例乘以最大栅格数，获取当前栅格位置
		
		GridN = (int)(((lat-South)/(North - South))*GetMaxGridN(100)) + 1;
		
		return GridN;
	}
	
	public static void main(String[] args)
	{	
		GISUtil ut = new GISUtil(531);
		//List<Integer> list = ut.GetGridMN(100,113.40021494074,23.090608903576);
		//System.out.println("M = " + list.get(0));
		//System.out.println("N = " + list.get(1));
		
		
		System.out.println(ut.GetGridID(100,116.927452,36.616216));
		//System.out.println(ut.GetGridID(100,113.36396099483,23.018637336017));
		//System.out.println(ut.GetGridID(100,113.32924230416,23.033854876418));
	}
}
