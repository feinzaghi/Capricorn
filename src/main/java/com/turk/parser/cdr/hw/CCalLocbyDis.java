package com.turk.parser.cdr.hw;

import org.apache.log4j.Logger;

import com.turk.util.LogMgr;


public class CCalLocbyDis {
	
	private static Logger log = LogMgr.getInstance().getSystemLogger();
	

	/**
     * 计算经纬度
     * @param ne
     * @param nDelay
     * @param flag
     * @param m_nPsmmNum
     * @param m_nPsmmCell
     * @param m_nPsmmSector
     * @return
     */
    public TDoublePoint CalLatAndLon(NEInfo ne,int nDelay,boolean flag
    		,int m_nPsmmNum,int[] m_nPsmmCell,int[] m_nPsmmSector)
    {
    	//String strTemp = ",,,,";
    	//String strLatLon=",,";
    	//String m_dRetstrLatLon=",,";
    	TDoublePoint dRet = new TDoublePoint();
    	//CMapWordToPtr map;
    	//void *pTemp=NULL;
    	//int nNum = m_nPsmmNum;//计算判断是否属于同一个基站
    	int nCount = 1;
    	//int nGridM=0,nGridN=0,GridID=0;
    	//int m_nCarr;
    	//double dwAngle = 0;//用于栅格计算

    	if(ne == null || ne.getCITY_ID() > 999 || m_nPsmmNum >6)
    	{   
    		//log.warn("网元配置不完整");
    		//m_dRetstrLatLon=",,";
    		//strLatLon=",,";
    		return null;
    	}

    	CCdlLoc[] m_arCdlLoc = new CCdlLoc[7];
    	
    	try
    	{
    		//map.SetAt(pObj->nBts,NULL);//把BTS增加到map表，便于判断是否此BTS已经出现过
    		if(nDelay>100)
    		{
    			nDelay = GetRandNum(70,100);
    		}
    		/////////////////////////////////////////
    		//主小区作为第一需要增加到小区列表中
    		CCdlLoc m_arCdlLoc0 = new CCdlLoc();
    		m_arCdlLoc0.dLen = (double)nDelay/8*244;
    		m_arCdlLoc0.nazimuth = ne.getANT_AZIMUTH();
    		m_arCdlLoc0.dLat = ne.getLatitude();
    		m_arCdlLoc0.dLon = ne.getLongitude();
    		m_arCdlLoc[0] = m_arCdlLoc0;
    		for(int i=0;i<m_nPsmmNum;i++)
    		{
    			//if(!map.Lookup(m_nPsmmCell[i],pTemp))//没找到相同的基站，开始增加到导频列表中去
    			//{
    			String nekey = String.format("%d_%d_%d_%d",ne.getBSC_ID(),m_nPsmmCell[i],m_nPsmmSector[i],ne.getCARR_ID());
    			NEInfo nepsmm = NEConfig.getInstance(ne.getCITY_ID())
				.getNEInfo(ne.getCITY_ID(), nekey);
    			
    			if(nepsmm!=null)
    			{
    				CCdlLoc m_arCdlLocpsmm = new CCdlLoc();
    				m_arCdlLocpsmm.dLen = (double)nDelay/8*244; //计算接入距离
    				m_arCdlLocpsmm.nazimuth = nepsmm.getANT_AZIMUTH();
    				m_arCdlLocpsmm.dLat = nepsmm.getLatitude();
    				m_arCdlLocpsmm.dLon = nepsmm.getLongitude();
    				m_arCdlLoc[nCount] = m_arCdlLocpsmm;
    				nCount ++;
    			}
    		}

    		int nCalCount = 0;
    		//strTemp=",,,,";
    		dRet = NewCalPointLoc(m_arCdlLoc,nCount,nCalCount);

    		/*
    		if(dRet == null)
    		{
    			String strWarn = String.format("LOCAL-NULL,P:[BSC:%d,CELL:%d,SECTOR:%d,CARR:%d] " +
    					"[PsmmNum:%d]", ne.getBSC_ID(),ne.getCELL_ID(),ne.getSECTOR_ID(),ne.getCARR_ID(),
    					m_nPsmmNum);
    			log.warn(strWarn);
    		}*/
    	}
    	catch(Exception e)
    	{
    		log.error("定位计算CalLatAndLon异常",e);
    	}
    	
    	return dRet;
    }

    	//取两个范围内的随机数
    private int GetRandNum(int minNum, int maxNum)
    {
    	return ((int)Math.random()%(maxNum-minNum))+minNum;
    }
    
    //根据有效的导频站计算经纬度
    //pCdlLoc 小区位置, nUsedCount 小区个数 dLoc 返回的经纬度，dLoc.x 经度
    //dLoc.y 纬度 nCalCount nCalCount[0] 返回有几个小区参与计算
    private TDoublePoint NewCalPointLoc(CCdlLoc pCdlLoc[], int nUsedCount ,
  								   int nCalCount) 
    {
  	
    	TDoublePoint dLoc = new TDoublePoint();
    	if(nUsedCount <= 0) 
    		return dLoc;
  	
	  	//取地图坐标系接入点做为平面坐标系下的圆点
	  	//double dCenterX = pCdlLoc[0].dLon ;
	  	//double dCenterY = pCdlLoc[0].dLat ;
	  	
	  	CMapLonLat pMapLon = new CMapLonLat();
  	
	  	TJWD pJwdCenter = pMapLon.CreateJwd(pCdlLoc[0].dLon,pCdlLoc[0].dLat);
	  	//double dR1 = pCdlLoc[0].dLen  ;
	  	//double dR2 = 0 ;
	  	TDoublePoint pStart = new TDoublePoint();
	  	pStart.x = 0 ;
	  	pStart.y = 0 ;
  	
	  	double dDis1 ;
	  	double dAngle = 0;
	  	//结果点
  	
	  	//交点
	  	TDoublePoint[] arrpCross = new TDoublePoint[100];
	  	for(int i = 0;i<100;i++)
	  	{
	  		TDoublePoint newpoint = new TDoublePoint();
	  		arrpCross[i] = newpoint;
	  	}
	  	//交点个数
	  	int nCrossPointCount = 0 ;
  	
	  	double dAccessLen = pCdlLoc[0].dLen ;
	  	//只有接入点,取接入距离，方向是小区方向角-随机值(30)
	  	if(nUsedCount == 1) 
	  	{
	  		nCalCount = 1 ;
	  		//private bool CalOnlyOneBts(double[] dLoc,double dAccessLen,
	  		//		TJWD pJwdCenter,double dAngle) 
	  		return CalOnlyOneBts(dAccessLen,pJwdCenter,pCdlLoc[0].nazimuth,pMapLon) ;
  		
	  	}
	  	else if(nUsedCount == 2) 
	  	{	//一个maho基站，一个圆和一个双曲线，作为两个圆求解
		  	//两圆没有交点，以一个站来处理
	  		//exit ;
	  		//TJWD pJwdTmp = pMapLon.CreateJwd(0.0,0.0);
	  		TJWD pJWdTmp1 = pMapLon.CreateJwd(0,0);
	  		pCdlLoc[0].dLon = 0 ;
	  		pCdlLoc[0].dLat = 0 ;
  		
	  		//arrpCross = expandArray(arrpCross,2);
	  		//基站点相对于接入点平面坐标的位置
	  		for(int j = 1;j< nUsedCount;j++)
	  		{
	  			pJWdTmp1.SetPoint(pCdlLoc[j].dLon,pCdlLoc[j].dLat);
	  			dDis1 = pMapLon.distance(pJwdCenter,pJWdTmp1) ;
	  			dAngle = pMapLon.getAngle(pJwdCenter,pJWdTmp1);
	  			pCdlLoc[j].dLon = dDis1 * Math.cos(dAngle * Math.PI / 180) ;
	  			pCdlLoc[j].dLat = dDis1 * Math.sin(dAngle * Math.PI / 180) ;
	  			pCdlLoc[j].drLen = dDis1 ;
	  		}
  		
	  		nCrossPointCount = CalTwoBtss(pCdlLoc,0,1,arrpCross);
	  		//两圆没有交点，以一个站来处理
	  		if(nCrossPointCount == 0) 
	  		{
	  			nCalCount = 1 ;
	  			return CalOnlyOneBts(dAccessLen,pJwdCenter,pCdlLoc[0].nazimuth,pMapLon);
	  		}
	  		else if(nCrossPointCount == 1) 
	  		{//只有一个交点
	  			dLoc.x = arrpCross[0].x;
	  			dLoc.y = arrpCross[0].y;
	  			//求交点角度
	  			dAngle = GetTwoPointsAngle(0,0,dLoc.x,dLoc.y);
	  			//距离
	  			dDis1 = Math.sqrt(dLoc.x * dLoc.x  + dLoc.y * dLoc.y);
	  			//TJWD pJwdStart = pMapLon.GetJWDB(pJwdCenter,dDis1,dAngle) ;
	  			nCalCount = 2 ;
	  			dLoc = CalOnlyOneBts(dDis1,pJwdCenter,dAngle,pMapLon);
	  			return dLoc;
	  		}
  		//两个点，根据方位
  		else if(nCrossPointCount == 2) 
  		{
  			return JudgeTwoCirclePoint(pCdlLoc,0,1,arrpCross,pJwdCenter,pMapLon);
  		} 
  	}
  	else if (nUsedCount >= 3) 
  	{	//有三个或以上的有效点
  		//TJWD pJwdTmp = pMapLon.CreateJwd(0,0);
  		TJWD pJWdTmp1 = pMapLon.CreateJwd(0,0);
  		pCdlLoc[0].dLon = 0 ;
  		pCdlLoc[0].dLat = 0 ;
  		
  		//基站点相对于接入点平面坐标的位置
  		double dx1,dy1,dy2, dx2 ;
  		for(int j = 1;j < nUsedCount ;j++)
  		{
  			try
  			{
  				pJWdTmp1.SetPoint(pCdlLoc[j].dLon,pCdlLoc[j].dLat);
  				dDis1 = pMapLon.distance(pJwdCenter,pJWdTmp1);
  				dAngle = pMapLon.getAngle(pJwdCenter,pJWdTmp1);
  				pCdlLoc[j].dLon = dDis1 * Math.cos(dAngle * Math.PI / 180) ;
  				pCdlLoc[j].dLat = dDis1 * Math.sin(dAngle * Math.PI / 180) ;
  				pCdlLoc[j].drLen = dDis1 ;
  				//相差距离
  				pCdlLoc[j].dPhaseLen = pCdlLoc[j].dLen - pCdlLoc[0].dLen ;
  			}
  			catch(Exception ex)
  			{
  				log.error(ex);
  				continue ;
  			}
  		}
  		
  		//两两双 曲线求交点
  		for(int i = 1 ; i <  nUsedCount - 1; i++)
  		{
  			for(int j = i + 1 ; j <  nUsedCount ;j++)
  			{
  				//两个双曲线求交点
  				{
  					
  					double[] arra = new double[4];
  					double[] arrb = new double[4];
  					//x2,1 y2,1 x3,1 y3,1的矩阵
  					arra[0] = pCdlLoc[i].dLon - pCdlLoc[0].dLon;
  					arra[1] = pCdlLoc[i].dLat - pCdlLoc[0].dLat;
  					arra[2] = pCdlLoc[j].dLon - pCdlLoc[0].dLon;
  					arra[3] = pCdlLoc[j].dLat - pCdlLoc[0].dLat;
  					//求逆矩阵
  					if(MatrixOpp(arra,arrb,2,2)) 
  					{
  						continue ;
  					}
  					double dc1,dc2,dc3,dc4 ;
  					double da1,da2 ;
  					//da, da1, da2,dc1,dc2  double ;
  					//(r2,1* r2,1 - r2 * r2)/ 2
  					dc1 = (pCdlLoc[i].dPhaseLen * pCdlLoc[i].dPhaseLen -
  						pCdlLoc[i].drLen * pCdlLoc[i].drLen) / 2 ;
  					//(r3,1 * r3,1 - r2 * r2) / 2
  					dc2 = (pCdlLoc[j].dPhaseLen * pCdlLoc[j].dPhaseLen -
  						pCdlLoc[j].drLen * pCdlLoc[j].drLen) / 2 ;
  					
  					dc3 = arrb[0] * dc1 + arrb[1] * dc2 ;
  					dc4 = arrb[2] * dc1 + arrb[3] * dc2 ;
  					da1 = arrb[0] * pCdlLoc[i].dPhaseLen + arrb[1] * pCdlLoc[j].dPhaseLen ;
  					da2 = arrb[2] * pCdlLoc[i].dPhaseLen + arrb[3] * pCdlLoc[j].dPhaseLen ;
  					
  					//求焦点
  					try
  					{
  						//系数为0
  						if(Math.abs(da1) < 0.0001) 
  						{
  							dx1 = dc3 ;
  							dx2 = dc3 ;
  							dLoc = GetTwoSqua(1-da2*da2,2*dc4,dc4*dc4-da2*da2*dc3*dc3);
  							if(dLoc == null) 
  							{
  								continue ;
  							}
  							
  							nCrossPointCount += 2;
  							//arrpCross = expandArray(arrpCross,nCrossPointCount);
  							arrpCross[nCrossPointCount - 2].x = dx1 ;
  							arrpCross[nCrossPointCount - 2].y = dLoc.x ;
  							arrpCross[nCrossPointCount - 1].x = dx2 ;
  							arrpCross[nCrossPointCount - 1].y = dLoc.y ;
  						}
  						else
  						{
  							
  							if (2*da2*dc4*da1*dc3-da2*da2*dc3*dc3-
  								da1*da1*dc4*dc4+dc4*dc4+dc3*dc3 < 0) 
  								continue ;
  							dx1=-1/2*da1/(da2*da2+da1*da1-1)*(-2*da2*dc4-2*da1*dc3+2*Math.sqrt(2*da2*dc4*da1*dc3-da2*da2*dc3*dc3-
  								da1*da1*dc4*dc4+dc4*dc4+dc3*dc3))-dc3 ;
  							dx2=-1/2*da1/(da2*da2+da1*da1-1)*(-2*da2*dc4-2*da1*dc3-2*Math.sqrt(2*da2*dc4*da1*dc3-da2*da2*dc3*dc3-
  								da1*da1*dc4*dc4+dc4*dc4+dc3*dc3))-dc3 ;
  							dy1 = (dx1 + dc3) * da2/da1 - dc4 ;
  							dy2 = (dx2 + dc3) * da2/da1 - dc4 ;
  							{
  								dx1 =(da1*da2*dc4-da1*Math.sqrt(2*da2*dc4*da1*dc3-dc3*dc3*da2*da2-
  									dc4*dc4*da1*da1+dc4*dc4+dc3*dc3)-dc3*da2*da2+dc3)/(da2*da2+da1*da1-1) ;
  								dx2 =(da1*da2*dc4+da1*Math.sqrt(2*da2*dc4*da1*dc3-dc3*dc3*da2*da2-
  									dc4*dc4*da1*da1+dc4*dc4+dc3*dc3)-dc3*da2*da2+dc3)/(da2*da2+da1*da1-1) ;
  								dy1 = (dx1 + dc3) * da2/da1 - dc4 ;
  								dy2 = (dx2 + dc3) * da2/da1 - dc4 ;
  							}
  							nCrossPointCount += 2;
  							//arrpCross = expandArray(arrpCross,nCrossPointCount);
  							arrpCross[nCrossPointCount - 2].x = dx1 ;
  							arrpCross[nCrossPointCount - 2].y = dy1 ;
  							arrpCross[nCrossPointCount - 1].x = dx2 ;
  							arrpCross[nCrossPointCount - 1].y = dy2 ;
  						}
  					}
  					catch(Exception ex)
  					{
  						log.error(ex);
  						continue ;
  					}
  				}
  			}
  		}
  		
  		//双曲线没有交点，则接入点进入计算,但是作为两圆圆求焦点
  		if(nCrossPointCount == 0) 
  		{
  			//两两求圆
  			for(int i = 0; i < nUsedCount - 1;i++)
  			{
  				for(int j = i + 1 ;j < nUsedCount ;j++)
  				{
  					nCrossPointCount = CalTwoBtss(pCdlLoc,i,j,arrpCross);
  				}
  			}
  			//如还没有，则作为一个站来处理
  			if(nCrossPointCount == 0) 
  			{
  				nCalCount = 1 ;
  				dLoc = CalOnlyOneBts(dAccessLen,
  	  					pJwdCenter,pCdlLoc[0].nazimuth,pMapLon);
  				return  dLoc;
  			}
  		}
  		
  		
  			nCalCount = nCrossPointCount ;
  			//如有交点，根据最小地乘法取点
  			return  JudgePoint(pCdlLoc,nUsedCount,arrpCross,nCrossPointCount,pJwdCenter,pMapLon) ;
  		}
	  	return dLoc ;
    }
    
   
    
    private int CalTwoBtss(CCdlLoc pCdlLoc[],int nIndex1,
			  int nIndex2,TDoublePoint arrpCross[]) 
    {

    	int nCrossPointCount = 0;
    	//TDoublePoint dLoc = new TDoublePoint(); 
    	TDoublePoint pStart = new TDoublePoint(); 
    	TDoublePoint pEnd = new TDoublePoint(); 
    	TDoublePoint pCross1 = new TDoublePoint(); 
    	TDoublePoint pCross2 = new TDoublePoint(); 
    	double dR1,dR2 ;

    	//圆1
    	pStart.x = pCdlLoc[nIndex1].dLon ;
    	pStart.y = pCdlLoc[nIndex1].dLat ;
    	dR1 = pCdlLoc[nIndex1].dLen ;

    	pEnd.x = pCdlLoc[nIndex2].dLon ;
    	pEnd.y = pCdlLoc[nIndex2].dLat ;
    	dR2 = pCdlLoc[nIndex2].dLen ;
    	boolean bOK = false ;
		//求交点 ,没有退出
		if(!CirInCirPoint(pStart,pEnd,dR1 ,dR2 ,pCross1,pCross2)) 
		{
			return nCrossPointCount;
		}

		bOK = false ;//不是同一个点
		if (Math.abs(pCross1.x-pCross2.x) < 0.0001 && 
				Math.abs(pCross1.y-pCross2.y) < 0.0001) 
		{
			bOK = true ;
		}
		//加入交点集
		//同一个点
		if(bOK) 
		{
			nCrossPointCount++ ;
			//arrpCross = expandArray(arrpCross,nCrossPointCount);
			arrpCross[nCrossPointCount - 1].x = pCross1.x ;
			arrpCross[nCrossPointCount - 1].y = pCross1.y ;
		}
		//不是同一个点
		else
		{
			nCrossPointCount += 2 ;
			//arrpCross = expandArray(arrpCross,nCrossPointCount);
			arrpCross[nCrossPointCount - 2].x = pCross1.x ;
			arrpCross[nCrossPointCount - 2].y = pCross1.y ;
			arrpCross[nCrossPointCount - 1].x = pCross2.x ;
			arrpCross[nCrossPointCount - 1].y = pCross2.y ;
		}
		return nCrossPointCount ;

    }
    
    private boolean CirInCirPoint(TDoublePoint p1,TDoublePoint p2,
			 double r1,double r2,TDoublePoint rp1 ,TDoublePoint rp2) 
    {

    	double a,b,r,delta ;
    	try
    	{
			a = p2.x-p1.x;
			b = p2.y-p1.y;
			r =(a*a+b*b+r1*r1-r2*r2)/2;
			if(a==0 &&b !=0) 
			{
				rp2.y = r/b;
				rp1.y = rp2.y;
				delta = r1*r1-rp1.y*rp1.y ;
				if(Math.abs(delta) < 0.000001) 
				{
					delta = 0 ;
				}
				else
				{
					if( delta < 0) 
						return false;
				}
				rp1.x = Math.sqrt(delta);
				rp2.x = -rp1.x;
			}
			else if(a!=0 && b==0) 
			{
				rp2.x = r/a;
				rp1.x = rp2.x;
				delta = r1*r1-rp1.x*rp2.x ;
				if(Math.abs(delta) < 0.000001) 
				{
					delta = 0 ;
				}
				else
				{
					if( delta < 0) 
						return false ;
				}
				rp1.y = Math.sqrt(delta);
				rp2.y = -rp1.y;
			}
			else if(a != 0 &&b !=0) 
			{
				delta = b*b*r*r-(a*a+b*b)*(r*r-r1*r1*a*a);
				if( Math.abs(delta) < 0.000001) 
				{
					delta = 0 ;
				}
				else
				{
					if( delta < 0) 
						return false ;
				}
				rp1.y = (b*r+Math.sqrt(delta))/(a*a+b*b);
				rp2.y = (b*r-Math.sqrt(delta))/(a*a+b*b);
				rp1.x = (r-b*rp1.y)/a;
				rp2.x = (r-b*rp2.y)/a;
			}
			rp1.x = rp1.x + p1.x;
			rp1.y = rp1.y + p1.y;
			rp2.x = rp2.x + p1.x;
			rp2.y = rp2.y + p1.y;
			return true ;
    	}
    	catch(Exception ex)
    	{
			rp1.x = 0;
			rp1.y = 0;
			rp2.x = 0;
			rp2.y = 0;
    	}
    	return false ;
    }
    
    private double GetTwoPointsAngle(double dStartX,double dStartY,
			   double dEndX,double dEndY ) 
    {

		double dx,dy,angle ;
		//double dDeta;
		dx = (dEndX - dStartX) ;
		dy = (dEndY - dStartY) ;
		//dDeta = Math.sqrt(dx * dx + dy * dy);

		if(Math.abs(dx) < 0.00001 )
			angle = 0 ;
		else
			angle = Math.atan(Math.abs(dy/dx))*180/Math.PI;

		if(dx > 0&&dy <= 0 )   //第四
		{
			angle = 360 - angle;
		}
		else if(dx <= 0&&dy < 0 )  //第三
		{
			angle = angle + 180.0;
		}
		else if(dx < 0&&dy >= 0 )  //第二
		{
			angle = 180 - angle;
		}
		return angle ;
    }
    
    //只有一个，在方向角加减30度的扇区里 随机产生一个点，当前就是取小区经纬度
    //参数 返回的经纬度，dAccessLen 距离，小区，方向角
    public  TDoublePoint CalOnlyOneBts(double dAccessLen,
  								 TJWD pJwdCenter,double dAngle,CMapLonLat pMapLon) 
    {
  	
    	TDoublePoint dLoc = new TDoublePoint();
  		//rand
  		TJWD dwRet = new TJWD(0,0);
  		double dTmp = dAngle + ((Math.random() % 110) - 55);

	  	if(dAccessLen > 100)
	  		dAccessLen = dAccessLen + ((Math.random() % 100) - 50);
	  	else
	  		dAccessLen = dAccessLen + (Math.random() % 50);
	  	///////////////////////////////////////////
	  	//如果距离过远，扇区收敛
	  	if(dAccessLen > 1500)
	  		dTmp = dAngle + ((Math.random() % 60) - 30);
  	
	  	dwRet = pMapLon.GetJWDB(pJwdCenter,dAccessLen,dTmp);		
	  	dLoc.x = dwRet.m_Longitude ;
  		dLoc.y = dwRet.m_Latitude ;
  		
  		return dLoc;
  	
    }
    
  //根据基站方位来判断有效交点
    private TDoublePoint JudgeTwoCirclePoint(CCdlLoc pCdlLoc[],
    									   int nIndex1,int nIndex2,TDoublePoint arrpCross[],
    									   TJWD pJwdCenter,CMapLonLat pMapLon) 
    {
    	TDoublePoint dLoc = new TDoublePoint();
    	double dAngle1,dAngle2,dAngle3,dAngle4 ;
    	boolean bOK1, bOK2, bOK3,bOK4 ;
    	int m,n ;
    	
    	//第一个点与导频1点的方向
    	dAngle1 = GetTwoPointsAngle(pCdlLoc[nIndex2].dLon,pCdlLoc[nIndex2].dLat,
    		arrpCross[0].x,arrpCross[0].y) ;
    	//第二个点与导频1点的方位
    	dAngle2 = GetTwoPointsAngle(pCdlLoc[nIndex2].dLon,pCdlLoc[nIndex2].dLat,
    		arrpCross[1].x,arrpCross[1].y) ;
    	
    	//第一个点与接入点的方向
    	dAngle3 = GetTwoPointsAngle(pCdlLoc[nIndex1].dLon,pCdlLoc[nIndex1].dLat,
    		arrpCross[0].x,arrpCross[0].y) ;
    	//第二个点与接入点的方向
    	dAngle4 = GetTwoPointsAngle(pCdlLoc[nIndex1].dLon,pCdlLoc[nIndex1].dLat,
    		arrpCross[1].x,arrpCross[1].y) ;
    	
    	bOK1 = false ;
    	bOK2 = false ;
    	bOK3 = false ;
    	bOK4 = false ;
    	
    	//接入点方位
    	m = (pCdlLoc[nIndex1].nazimuth) % 360 ;
    	//导频1点方位
    	n = (pCdlLoc[nIndex2].nazimuth) % 360 ;
    	
    	double dAngle = 0;
    	//第一个点与接入点比较
    	if(GetTwoAnglesPhase(dAngle3,m) <= 60) 
    	{
    		dLoc.x = arrpCross[0].x ;
    		dLoc.y = arrpCross[0].y ;
    		dAngle = dAngle3;
    		bOK1 = true;
    	}
    	//第一个点与导频1比较
    	if(GetTwoAnglesPhase(dAngle1 ,n) <=60)  
    	{
    		dLoc.x = arrpCross[0].x ;
    		dLoc.y = arrpCross[0].y ;
    		dAngle = dAngle3;
    		bOK2 = true;
    	}
    	
    	double dDis1 ;
    	//如果在两个之内则取此点
    	if (bOK1 && bOK2) 
    	{
    		dDis1 = Math.sqrt(dLoc.x * dLoc.x + dLoc.y * dLoc.y)  ;
    		//TJWD pJwdStart = pMapLon.GetJWDB(pJwdCenter,dDis1,dAngle) ;
    		//dLoc->x = pJwdStart.m_Longitude ;
    		//dLoc->y = pJwdStart.m_Latitude ;
    		dLoc = CalOnlyOneBts(dDis1,pJwdCenter,dAngle,pMapLon) ;
    		
    		return dLoc ;
    	}
    	
    	//第二个点与接入点比较
    	if(GetTwoAnglesPhase(dAngle4,m) <= 60 )
    	{
    		dLoc.x = arrpCross[1].x ;
    		dLoc.y = arrpCross[1].y ;
    		dAngle = dAngle4;
    		bOK3 = true ;
    	}
    	//第一个点与导频1比较
    	if(GetTwoAnglesPhase(dAngle2 ,n) <= 60) 
    	{
    		dLoc.x = arrpCross[1].x ;
    		dLoc.y = arrpCross[1].y ;
    		dAngle = dAngle4;
    		bOK4 = true;
    	}
    	//如果在两个之内则取此点
    	if (bOK3 && bOK4) 
    	{
    		dDis1 = Math.sqrt(dLoc.x * dLoc.x + dLoc.y * dLoc.y)  ;
    		dLoc = CalOnlyOneBts(dDis1,pJwdCenter,dAngle,pMapLon) ;
    		
    		//TJWD pJwdStart = pMapLon.GetJWDB(pJwdCenter,dDis1,dAngle) ;
    		//dLoc->x = pJwdStart.m_Longitude ;
    		//dLoc->y = pJwdStart.m_Latitude ;
    		return dLoc ;
    	}
    	
    	//都不是 则取到接入点或导频1点 方向最小的
    	if(GetTwoAnglesPhase(dAngle1,n) + GetTwoAnglesPhase(dAngle3,m) <
    		GetTwoAnglesPhase(dAngle2,n) + GetTwoAnglesPhase(dAngle4,m)) 
    	{
    		dLoc.x = arrpCross[0].x ;
    		dLoc.y = arrpCross[0].y ;
    		dAngle = dAngle3;
    	}
    	else
    	{
    		dLoc.x = arrpCross[1].x ;
    		dLoc.y = arrpCross[1].y ;
    		dAngle = dAngle4;
    	}
    	dDis1 = Math.sqrt(dLoc.x * dLoc.x + dLoc.y * dLoc.y)  ;
    	
    	dLoc =  CalOnlyOneBts(dDis1,pJwdCenter,dAngle,pMapLon) ;
    	return dLoc ;
    }
    
  //取的两个角的差
    private double GetTwoAnglesPhase(double AngleA,double AngleB) 
    {
    	
    	double nMin, nMax ;
    	if(AngleA < AngleB) 
    	{
    		nMin = AngleA ;
    		nMax = AngleB ;
    	}
    	else
    	{
    		nMin = AngleB ;
    		nMax = AngleA ;
    	}
    	
    	if (nMax - nMin > 180) 
    	{
    		return 360 - nMax + nMin ;
    	}
    	else
    	{
    		return nMax - nMin ;
    	}
    }

    //矩阵求逆
    boolean MatrixOpp(double arrA[],double arrB[] ,int m,int n)
    {
    	
    	int i,j,x,y,k ;
    	double dx ;
    	
    	double[] sp = new double[m*n];
    	double[] AB = new double[m*n];
    	double[] b  = new double[m*n];

    	boolean bResult;
    	
    	if (m == 0 ||n == 0) 
    	{
    		return false;
    	}
    	
    	dx = Surplus(arrA,m,n);
    	if(Math.abs(dx) < 0.0001) 
    	{
    		//delete sp;delete AB;delete b;
    		return false ;
    	}
    	dx = 1 / dx ;
    	
    	for(i = 0 ;i < m ;i++)
    	{
    		for( j = 0 ;j<  n ;j++)
    		{
    			for( k = 0 ;k <m * n ;k++)
    				b[k] = arrA[k] ;
    			for(x = 0 ;x <n ;x++)
    				b[i *n + x] = 0 ;
    			for(y = 0;y< m ;y++)
    				b[m * y + j] = 0 ;
    			b[i*n+j] = 1;
    			sp[i*n+j] = Surplus(b,m,n);
    			AB[i*n+j] = dx*sp[i*n+j];
    		}
    	}
    	bResult = MatrixInver(AB,arrB, m,n);
    	//delete sp;delete AB;delete b;
    	return bResult;
    	
    }
 
    //求矩阵行列式
    double Surplus(double arrA[] ,int m,int n )
    {
    	int i,j ,k ;
    	double temp,temp1,x = 0;
    	temp = 1 ;
    	temp1 = 1;
    	
    	if(n== 2) 
    	{
    		for(i = 0;i <m; i++)
    		{
    			for(j = 0;j < n;j++)
    			{
    				if((i + j) % 2 ==  1) 
    					temp1 = temp1 * arrA[i * n + j];
    				else
    					temp = temp * arrA[i * n + j] ;
    			}
    		}
    		x = temp - temp1 ;
    	}
    	else
    	{
    		for(k = 0;k< n;k++)
    		{
    			j = k ;
    			for(i = 0 ; i <m ;i++)
    			{
    				if(j >= n) 
    					break ;
    				temp = temp * arrA[i * n + j] ;
    			} 
    		} 
    	} 
    	return x ;
    	
    } 
    
    //矩阵转置
    boolean MatrixInver(double arrA[] ,double arrB[],int m,int n )
    {
    	
    	if (m == 0 || n == 0) 
    	{
    		return false;
    	}
    	for(int i = 0;i< m;i++)
    	{
    		for(int j = 0 ; j < n;j++)
    		{
    			arrB[i * m + j] = arrA[j * n + i];
    		}
    	}
    	return  true ;
    	
    }
    
    //求救二元一次方程
    TDoublePoint GetTwoSqua(double a,double b,double c)
    {
    	TDoublePoint ddel = new TDoublePoint();
    	if(b * b - 4 * a * c < 0)
    	{
    		return null;
    	}
    	ddel.x = (-b + Math.sqrt(b * b - 4*a*c))/(2*a) ;
    	ddel.y = (-b - Math.sqrt(b * b - 4*a*c))/(2*a) ;
    	return ddel;
    }
    
    //根据最小地乘法取点
    //参数 经纬度,传入的小区信息,有效小区数，交点集,交点个数，中心点(主小区信息)
    TDoublePoint JudgePoint(CCdlLoc pCdlLoc[], int nUsedCount,
  							  TDoublePoint arrpCross[],
  							  int nCrossPointCount,TJWD pJwdCenter,CMapLonLat pMapLon) 
 	{
    	TDoublePoint dLoc = new TDoublePoint();
    	int nPointIndex = -1 ;
    	double dMin = 999999999 ;
    	double dLen = 0, dTmpLen ;
    	//理论 MIn(Ri - ||Xi - Xs||) * (Ri - ||Xi - Xs||) 和最小的
    	for(int m= 0;m<nCrossPointCount;m++ )
    	{
    		dLen = 0 ;
    		try
    		{
    			for(int n = 0 ; n< nUsedCount;n++)
    			{
    				//(Xi - Xs)
    				dTmpLen = Math.sqrt((pCdlLoc[n].dLon - arrpCross[m].x) *
	  					(pCdlLoc[n].dLon - arrpCross[m].x) +
	  					(pCdlLoc[n].dLat - arrpCross[m].y) *
	  					(pCdlLoc[n].dLat - arrpCross[m].y));
    				//(Ri - ||Xi - Xs||)* (Ri - ||Xi - Xs||)
    				dLen = dLen + (dTmpLen - pCdlLoc[n].dLen) *
  						(dTmpLen - pCdlLoc[n].dLen) ;
    			}
	  			//取距离最小的序号
	  			if(dLen < dMin) 
	  			{
	  				dMin = dLen ;
	  				nPointIndex = m ;
	  			}
	  		}
	  		catch(Exception ex)
	  		{
	  			log.error(ex);
	  			dLen = 0;
	  		}
	  	}
	  	if(nPointIndex == -1) 
	  	{
	  		return null;
	  	}
  	
	  	double dAngle,dDis1 ;
	  	//nPointIndex = 0 ;
	  	//平面坐标系下交点相对于接入点方位
	  	dAngle = GetTwoPointsAngle(0,0,arrpCross[nPointIndex].x,arrpCross[nPointIndex].y) ;
	  	//平面坐标系下交点相对于接入点距离
	  	dDis1 = Math.sqrt(arrpCross[nPointIndex].x * arrpCross[nPointIndex].x +
  		arrpCross[nPointIndex].y * arrpCross[nPointIndex].y) ;
	  	//转换为地图经纬度
	  	dLoc = CalOnlyOneBts(dDis1,pJwdCenter,dAngle,pMapLon) ;
	  	return dLoc ;
  	
 	}
}

    
    
    class CCdlLoc
    {
    	public double	dLen ;   //接入点到小区的距离
    	public double	drLen ;  //小区与主站的距离 --不要赋值，计算得来
    	public int		nazimuth ; //方向角
    	public double	dPhaseLen ; //距离差 不要赋值，计算得来    	
    	public double	dLon,dLat ; //小区经纬度，及地图坐标换成平面坐标时相对位置
    }