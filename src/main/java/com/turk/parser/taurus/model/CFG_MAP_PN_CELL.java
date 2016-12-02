package com.turk.parser.taurus.model;

public class CFG_MAP_PN_CELL {
	//cell_name
	//Longitude
	//Latitude
	//PN
	//cell_sys_id
	
	private String _cellname;
	
	public String getCellName()
	{
		return this._cellname;
	}
	
	public void setCellName(String cellname)
	{
		this._cellname = cellname;
	}
	
	private double _longitude;
	
	public double getLongitude()
	{
		return this._longitude;
	}
	
	public void setLongitude(double longitude)
	{
		this._longitude = longitude;
	}
	
	private double _latitude;
	
	public double getLatitude()
	{
		return this._latitude;
	}
	
	public void setLatitude(double latitude)
	{
		this._latitude = latitude;
	}
	
	private int _pn;
	
	public int getPN()
	{
		return this._pn;
	}
	
	public void setPN(int pn)
	{
		this._pn = pn;
	}
	
	private long _cellsysid;
	
	public long getCellSysID()
	{
		return this._cellsysid;
	}
	
	public void setCellSysID(long cellsysid)
	{
		this._cellsysid = cellsysid;
	}

}
