package com.turk.specialapp;

import java.util.Date;

/**
 * 话单一天完成情况信息
 * @author Administrator
 *
 */
public class CdrFileDayInfo {

	private Date _starttime;
	
	public Date getStartTime()
	{
		return _starttime;
	}
	
	public void setStartTime(Date starttime)
	{
		this._starttime = starttime;
	}
	
	private int _cityID;
	
	public int getCityID()
	{
		return this._cityID;
	}
	
	public void setCityID(int cityid)
	{
		this._cityID = cityid;
	}
	
	private int _count;
	
	public int getCount()
	{
		return this._count;
	}
	
	public void setCount(int count)
	{
		this._count = count;
	}
	
	private Date _stamptime;
	
	public Date getStampTime()
	{
		return this._stamptime;
	}
	
	public void setStampTime(Date stamptime)
	{
		this._stamptime = stamptime;
	}
	
}
