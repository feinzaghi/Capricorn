package com.turk.util;

public class FTPInfo
{
	private String _ip;
	private int _port;
	private String _user;
	private String _pwd;
	private String _encode;
	
	public void setIP(String ip)
	{
		_ip = ip;
	}
	
	public String getIP()
	{
		return _ip;
	}
	
	public int getPort()
	{
		return _port;
	}
	public void setPort(int port)
	{
		_port = port;
	}
	
	public void setUser(String user)
	{
		_user = user;
	}
	public String getUser()
	{
		return _user;
	}
	
	public void setPwd(String pwd)
	{
		_pwd = pwd;
	}
	public String getPwd()
	{
		return _pwd;
	}
	
	public String getEncode()
	{
		return _encode;
	}
	
	public void setEncode(String encode)
	{
		_encode = encode;
	}
}
