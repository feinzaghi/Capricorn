package com.turk.parser.taurus;

public class ScoketMsg {

	private int _mobileeventtype;
	
	public int getMoblieEventType()
	{
		return this._mobileeventtype;
	}
	
	public void setMobileEventType(int mobileeventtype)
	{
		this._mobileeventtype = mobileeventtype;
	}
	
	private String _data;
	
	public String getData()
	{
		return this._data;
	}
	
	public void setData(String data)
	{
		this._data = data;
	}
	
	private String _hexbody;
	
	public String getHexbody()
	{
		return this._hexbody;
	}
	
	public void setHexbody(String hexbody)
	{
		this._hexbody = hexbody;
	}
}
