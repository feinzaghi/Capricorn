package com.turk.task;

/**
 * 采集设备对象
 * @author Administrator
 *
 */
public class DevInfo
{
	private int devid;
	private String name;
	private String devicename;
	private String ip;
	private String hostUser;
	private String hostPwd;
	private String hostSign;
	//private int omcID;
	//private String deviceName;
	private int cityID;
	private String vendor;
	private String encode;

	public int getDevID()
	{
		return this.devid;
	}

	public void setDevID(int id)
	{
		this.devid = id;
	}

	public String getName()
	{
		return this.name;
	}	

	public void setName(String name)
	{
		this.name = name;
	}

	public String getIP()
	{
		return this.ip;
	}

	public void setIP(String ip)
	{
		this.ip = ip;
	}

	public String getHostUser()
	{
		return this.hostUser;
	}

	public void setHostUser(String user)
	{
		this.hostUser = user;
	}

	public String getHostPwd()
	{
		return this.hostPwd;
	}

	public void setHostPwd(String pwd)
	{
		this.hostPwd = pwd;
	}

	public String getHostSign()
	{
		return this.hostSign;
	}

	public void setHostSign(String sign)
	{
		this.hostSign = sign;
	}

	public void setDeviceName(String deviceName)
	{
		this.devicename = deviceName;
	}

	public String getDeviceName()
	{
		return this.devicename;
	}

	public void setCityID(int cityID)
	{
		this.cityID = cityID;
	}

	public int getCityID()
	{
		return this.cityID;
	}

	public String getVendor()
	{
		return this.vendor;
	}

	public void setVendor(String vendor)
	{
		this.vendor = vendor;
	}

	public String getEncode()
	{
		return this.encode;
	}

	public void setEncode(String encode)
	{
		this.encode = encode;
	}
}