package com.turk.parser.cdr.hw;

public class TJWD {
	
	public double m_LoDeg, m_LoMin, m_LoSec ; // longtitude 经度
	public double m_LaDeg, m_LaMin, m_LaSec ;
	public double m_Longitude, m_Latitude ;
	public double m_RadLo, m_RadLa  ;
	public double Ec, Ed ;
	
	
	double Rc = 6378137.00 ; // 赤道半径
	double Rj = 6356725; // 极半径
	//double PI = 3.14159262 ;
	double E_S_Rad = 40408299981544.355;
	double E_L_Rad = 40680631590769.0;
	
	public TJWD(double longitude, double latitude)
	{
		SetPoint(longitude, latitude);
	}
	
	public void SetPoint(double longitude, double latitude)
	{
		  m_LoDeg = (int)longitude;
		  m_LoMin = (int)(longitude - m_LoDeg)*60;
		  m_LoSec = (longitude - m_LoDeg - m_LoMin/60)*3600;
		  
		  m_LaDeg = (int)(latitude);
		  m_LaMin = (int)((latitude - m_LaDeg)*60);
		  m_LaSec = (latitude - m_LaDeg - m_LaMin/60)*3600;
		  
		  m_Longitude = longitude;
		  m_Latitude = latitude;
		  m_RadLo = longitude * Math.PI/180;
		  m_RadLa = latitude * Math.PI/180;
		  Ec = Rj + (Rc - Rj) * (90.-m_Latitude) / 90.;
		  Ed = Ec * Math.cos(m_RadLa);	  
	}
	
	
}
