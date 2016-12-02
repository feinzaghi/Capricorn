package com.turk.db.pojo;

public class Templet
{
	private int tmpID;
	private int tmpType;
	private String tmpName;
	private String edition;
	private String tempFileName;

	public Templet()
	{
  	}

	public Templet(int tmpID, int tmpType, String tmpName, String edition, String tempFileName)
	{
		this.tmpID = tmpID;
		this.tmpType = tmpType;
		this.tmpName = tmpName;
		this.edition = edition;
		this.tempFileName = tempFileName;
	}

	public int getTmpID()
	{
		return this.tmpID;
	}

	public void setTmpID(int tmpID)
	{
		this.tmpID = tmpID;
	}

	public int getTmpType()
	{
		return this.tmpType;
	}

	public void setTmpType(int tmpType)
	{
		this.tmpType = tmpType;
	}

	public String getTmpName()
	{
		return this.tmpName;
	}

	public void setTmpName(String tmpName)
	{
		this.tmpName = tmpName;
	}

	public String getEdition()
	{
		return this.edition;
	}

	public void setEdition(String edition)
	{
		this.edition = edition;
	}

	public String getTempFileName()
	{
		return this.tempFileName;
	}

	public void setTempFileName(String tempFileName)
	{
		this.tempFileName = tempFileName;
	}
}