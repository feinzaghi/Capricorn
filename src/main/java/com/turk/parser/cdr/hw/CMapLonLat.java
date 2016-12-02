package com.turk.parser.cdr.hw;

public class CMapLonLat {

	double E_S_Rad = 40408299981544.355;
	double E_L_Rad = 40680631590769.0;
	
	public TJWD CreateJwd(double longitude, double latitude)
	{
		TJWD pJwd = new TJWD(longitude,latitude);
		return pJwd; 
	}
	
	public double distance(TJWD A, TJWD B)  
	{
		//double angle = Double.parseDouble(objangle.toString());
		double dbfLatDiff = (B.m_RadLa - A.m_RadLa);
		double dbfLongDiff = (B.m_RadLo - A.m_RadLo);
		double dbltmp = Math.cos(A.m_RadLa);
		double tmp = Math.sin(A.m_RadLa);
		tmp = E_S_Rad+(E_L_Rad-E_S_Rad)*tmp*tmp;
		tmp = E_S_Rad*E_L_Rad/tmp;
		double dDeta = Math.sqrt(tmp*(dbfLongDiff*dbfLongDiff*dbltmp*dbltmp+dbfLatDiff*dbfLatDiff));
		return dDeta ;	  
	}
	
	public double getAngle(TJWD A, TJWD B)
	{
		double angle = 0;
		double dx, dy ;
		double dLo , dLa ;
		dx = (B.m_RadLo - A.m_RadLo) * A.Ed;
		dy = (B.m_RadLa - A.m_RadLa) * A.Ec;
		  
		if(Math.abs(dx) < 0.00001)
			angle = 0 ;
		else
			angle = Math.atan(Math.abs(dy/dx))*180/Math.PI;
		// 判断象限
		dLo = B.m_Longitude - A.m_Longitude;
		dLa = B.m_Latitude - A.m_Latitude;
		if(dLo > 0&&dLa <= 0)    //第四
		{
			angle = 360 - angle;
		}
		else if(dLo <= 0&&dLa < 0)   //第三
		{
			angle = angle + 180.0;
		}
		else if(dLo < 0&&dLa >= 0)   //第二
		{
			angle = 180 - angle;
		}
		return angle;
	}
	
	public TJWD GetJWDB(TJWD A, double distance, double angle)
	{
		double dx, dy ,BJD ,BWD ;
			  //		  TJWD LocJwd   ;
		 dx = distance * Math.cos(angle * Math.PI /180.0);
		 dy = distance * Math.sin(angle * Math.PI /180.0);
			  
		 BJD = (dx/A.Ed + A.m_RadLo) * 180.0/Math.PI;
		 BWD = (dy/A.Ec + A.m_RadLa) * 180.0/Math.PI;
		 TJWD pJwdStart = new TJWD(BJD, BWD);
		 return pJwdStart ;
	}
}
