package com.turk.db.pojo;

/**
 * 采集设备
 * @author Administrator
 *
 */
public class Device
{
	private int devID;
	private String devName;
	private int cityID;
	private int omcID;
	private String vendor;
 	private String hostIP;
 	private String hostUser;
 	private String hostPwd;
 	private String hostSign;

 	public int getDevID()
 	{
 		return this.devID;
 	}

 	public void setDevID(int devID)
 	{
 		this.devID = devID;
 	}

 	public String getDevName()
 	{
 		return this.devName;
 	}

 	public void setDevName(String devName)
 	{
 		this.devName = devName;
 	}

 	public int getCityID()
 	{
 		return this.cityID;
 	}

 	public void setCityID(int cityID)
 	{
 		this.cityID = cityID;
 	}

 	public int getOmcID()
 	{
 		return this.omcID;
 	}

 	public void setOmcID(int omcID)
 	{
 		this.omcID = omcID;
 	}

 	public String getVendor()
 	{
 		return this.vendor;
 	}

 	public void setVendor(String vendor)
 	{
 		this.vendor = vendor;
 	}

 	public String getHostIP()
 	{
 		return this.hostIP;
 	}

 	public void setHostIP(String hostIP)
 	{
 		this.hostIP = hostIP;
 	}

 	public String getHostUser()
 	{
 		return this.hostUser;
 	}

 	public void setHostUser(String hostUser)
 	{
 		this.hostUser = hostUser;
 	}	

 	public String getHostPwd()
 	{
 		return this.hostPwd;
 	}

 	public void setHostPwd(String hostPwd)
 	{
 		this.hostPwd = hostPwd;
 	}

 	public String getHostSign()
 	{
 		return this.hostSign;
 	}

 	public void setHostSign(String hostSign)
 	{
 		this.hostSign = hostSign;
 	}

 	public static void main(String[] args)
  	{
  	}
}