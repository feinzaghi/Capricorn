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
 * GIS���Ӧ�÷���
 */
public class GISUtil {

	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * ����
	 * ��ʼ����������GIS����
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
			
			//�޸�Ϊ�Ե�ͼ 0��0��Ϊ��ʼԭ��
			East = longitude_r;
			West = longitude_l;
			South = latitude_l;
			North = latitude_r;
			
			//���㳤��
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
	 * ���г����ף�
	 */
	public double MaxLong;
	
	/**
	 * ���п��ף�
	 */
	public double MaxWidth;
	
	/**
	 * ���ж��߾���
	 */
	public double East = 0;
	
	/**
	 * �������߾���
	 */
	public double West = 0;
	
	/**
	 * �����ϱ�ά��
	 */
	public double South = 0;
	
	/**
	 * ���б���ά��
	 */
	public double North = 0;
	
	public int CityID = 0;
	
	/**
	 * ��ȡ�õ��������դ��Mֵ
	 * @param  meter �����ӵı߳�
	 */
	public int GetMaxGridM(int meter,double lon,double lat)
	{
		//�������դ��ʱ��Ҫͳ�Ƶ�ǰγ�ȵľ���
		double curLong = GetDistance(West,lat,East,lat);
		_MaxGridM = (int)(curLong/meter)+1;
		return _MaxGridM;
	}
	
	/**
	 * ��ȡ�õ��������դ��Nֵ
	 */
	public int GetMaxGridN(int meter)
	{
		_MaxGridN = (int)(MaxWidth/meter)+1;
		return _MaxGridN;
	}
	
	private static double EARTH_RADIUS = 6378137;  
	
	/**
	 * �������㾭γ�ȣ���ȡ����֮��ľ���(��)
	 * @param  lon1 ��һ����
	 * @param  lat1 ��һά��
	 * @param  lon2 �������
	 * @param  lat2 ���ά��
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
	      s = s * 6378137.0;// ȡWGS84��׼�ο������еĵ��򳤰뾶(��λ:m)
	      s = Math.round(s * 10000) / 10000;

	      return s; 
		 
		 double C = Math.sin(lat1*Math.PI/180)*Math.sin(lat2*Math.PI/180) 
		 + Math.cos(lat1*Math.PI/180)*Math.cos(lat2*Math.PI/180)*Math.cos(Math.abs(lon1-lon2)*Math.PI/180);

		 double Distance = EARTH_RADIUS*Math.asin(C)*Math.PI/180;
		 
		 return Distance;*/
	}
	
	/**
	 * ���ݵ�ľ�γ�ȣ���ȡ�õ��ڵ�ǰ���е�դ��ID��
	 * M*10000+N ͳһ����ϵ�����½ǿ�ʼ����
	 * @param  metar ÿ��դ��ı߳�
	 * @param  lon ����
	 * @param  lat ά��
	 */
	public long GetGridID(int metar,double lon,double lat)
	{
		long GridID = 0;
		long M = 0;
		long N = 0;
		
		//���ݵ�ľ��ȣ���ȡ���ڵ�ǰ����ȫ��դ���е�ͬһά�����ϵ�λ�ñ����������դ��������ȡ��ǰդ��λ��
		M = (long)(((double)(lon - West)/(East - West))*GetMaxGridM(metar,lon,lat)) + 1;
		//M = (long)(GetDistance(West,lat,lon,lat)/100) + 1;
		
		N = (long)(((lat - South)/(North - South))*GetMaxGridN(metar))+1;
		//N = (long)(GetDistance(lon,South,lon,lat)/100) + 1;
		
		GridID = M*10000+N;
		return GridID;
	}
	
	/**
	 * ͨ����γ�Ȼ�ȡM,Nֵ��ͨ�����鷵�أ�M 0,N 1
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
		
		//���ݵ�ľ��ȣ���ȡ���ڵ�ǰ����ȫ��դ���е�ͬһά�����ϵ�λ�ñ����������դ��������ȡ��ǰդ��λ��
		M = (int)(((lon-West)/(East - West))*GetMaxGridM(metar,lon,lat)) + 1;
		
		N = (int)(((lat-South)/(North - South))*GetMaxGridN(metar)) + 1;
		
		MN.add(M);
		MN.add(N);
		return MN;
	}
	
	/**
	 * ͨ����γ�Ȼ�ȡM,Nֵ��ͨ�����鷵�أ�M 0,N 1(Ĭ��100mդ��)
	 * @param lon
	 * @param lat
	 * @return
	 */
	public List<Integer> GetGridMN(double lon,double lat)
	{
		List<Integer> MN = new ArrayList<Integer>();
		int M = 0;
		int N = 0;
		
		//���ݵ�ľ��ȣ���ȡ���ڵ�ǰ����ȫ��դ���е�ͬһά�����ϵ�λ�ñ����������դ��������ȡ��ǰդ��λ��
		M = (int)(((lon-West)/(East - West))*GetMaxGridM(100,lon,lat)) + 1;
		
		N = (int)(((lat-South)/(North - South))*GetMaxGridN(100)) + 1;
		
		MN.add(M);
		MN.add(N);
		return MN;
	}
	
	public int GetGridM(double lon,double lat)
	{
		int GridM = 0;
		//���ݵ�ľ��ȣ���ȡ���ڵ�ǰ����ȫ��դ���е�ͬһά�����ϵ�λ�ñ����������դ��������ȡ��ǰդ��λ��
		GridM = (int)(((lon-West)/(East - West))*GetMaxGridM(100,lon,lat)) + 1;
		
		return GridM;
	}
	
	public int GetGridN(double lon,double lat)
	{
		int GridN = 0;
		
		//���ݵ�ľ��ȣ���ȡ���ڵ�ǰ����ȫ��դ���е�ͬһά�����ϵ�λ�ñ����������դ��������ȡ��ǰդ��λ��
		
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
