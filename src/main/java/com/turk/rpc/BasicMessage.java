package com.turk.rpc;

import java.util.HashSet;

public class BasicMessage {
	
	public static BasicMessage _instance = null;
	
	public static BasicMessage getInstance()
	{
		if(_instance == null)
			_instance = new BasicMessage();
		return _instance;
	}
	
	private HashSet<String> _ipFilterForLog = new HashSet<String>();
	
	public void setIPFilterForLog(HashSet<String> ipFilterForLog)
	{
		_ipFilterForLog = ipFilterForLog;
	}
	
	public HashSet<String> getIPFilterForLog()
	{
		return _ipFilterForLog;
	}
	
	private HashSet<String> _ipWhiteList = new HashSet<String>();
	
	public void setIPWhiteList(HashSet<String> ipWhiteList)
	{
		_ipWhiteList = ipWhiteList;
	}
	
	public HashSet<String> getIPWhiteList()
	{
		return _ipWhiteList;
	}

}
