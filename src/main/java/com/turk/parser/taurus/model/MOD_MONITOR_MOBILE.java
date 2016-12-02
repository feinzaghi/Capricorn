package com.turk.parser.taurus.model;

import java.util.Date;
import java.util.HashMap;

public class MOD_MONITOR_MOBILE {
	//CASE_ID,MONITOR_NAME,START_TIME,END_TIME," +
	//"NUM_TYPE,NUM_LIST,ALARM_LEVEL "
	
	private int _monitorid;
	
	public int getMonitorID()
	{
		return this._monitorid;
	}
	
	public void setMonitorID(int monitorid)
	{
		this._monitorid = monitorid;
	}
	
	private String _caseid;
	
	public String getCaseID()
	{
		return this._caseid;
	}
	
	public void setCaseID(String caseid)
	{
		this._caseid = caseid;
	}
	
	private String _monitorname;
	
	public String getMonitorName()
	{
		return this._monitorname;
	}
	
	public void setMonitorName(String monitorname)
	{
		this._monitorname = monitorname;
	}
	
	private Date _starttime;
	
	public Date getStartTime()
	{
		return this._starttime;
	}
	
	public void setStartTime(Date starttime)
	{
		this._starttime = starttime;
	}
	
	private Date _endtime;
	
	public Date getEndTime()
	{
		return this._endtime;
	}
	
	public void setEndTime(Date endtime)
	{
		this._endtime = endtime;
	}
	
	private String _numtype;
	
	public String getNumType()
	{
		return this._numtype;
	}
	
	public void setNumType(String numtype)
	{
		this._numtype = numtype;
	}
	
	private String _numlist;
	
	public String getNumList()
	{
		return this._numlist;
	}
	
	public void setNumList(String numlist)
	{
		this._numlist = numlist;
	}
	
	private String _alarmlevel;
	
	public String getAlarmLevel()
	{
		return this._alarmlevel;
	}
	
	public void setAalarmLevel(String alarmlevel)
	{
		this._alarmlevel = alarmlevel;
	}
	
	private String _callnumlist;
	
	public String getCallNumList()
	{
		return this._callnumlist;
	}
	
	public void setCallNumList(String callnumlist)
	{
		this._callnumlist = callnumlist;
	}
	
	private String _exitareacelllist;
	
	public String getExitAreaCellList()
	{
		return this._exitareacelllist;
	}
	
	public void setExitAreaCellList(String exitareacelllist)
	{
		this._exitareacelllist = exitareacelllist;
	}
	
	private String _enterareacelllist;
	
	public String getEnterAreaCellList()
	{
		return this._enterareacelllist;
	}
	
	public void setEnterAreaCellList(String enterareacelllist)
	{
		this._enterareacelllist = enterareacelllist;
	}
	
	
	
	private HashMap<Integer,String> _exitLAClist;
	
	public HashMap<Integer,String> getExitLACList()
	{
		return this._exitLAClist;
	}
	
	public void setExitLACList(HashMap<Integer,String> exitlaclist)
	{
		this._exitLAClist = exitlaclist;
	}
	
	private HashMap<Integer,String> _enterlaclist;
	
	public HashMap<Integer,String> getEnterLACList()
	{
		return this._enterlaclist;
	}
	
	public void setEnterLACList(HashMap<Integer,String> enterlaclist)
	{
		this._enterlaclist = enterlaclist;
	}
	
	private String _username;
	
	public String getUserName()
	{
		return this._username;
	}
	
	public void setUserName(String username)
	{
		this._username = username;
	}
	
	private String _sendmsglist;
	
	public String getSendMsgList()
	{
		return this._sendmsglist;
	}
	
	public void setSendMsgList(String sendmsglist)
	{
		this._sendmsglist = sendmsglist;
	}
	
	private HashMap<String,String> _excludenumlist;
	
	public HashMap<String,String> getExcludeNumList()
	{
		return this._excludenumlist;
	}
	
	public void setExcludeNumList(HashMap<String,String> excludenumlist)
	{
		this._excludenumlist = excludenumlist;
	}
}
