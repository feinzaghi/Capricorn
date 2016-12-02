package com.turk.db.pojo;

import java.util.Date;
import com.turk.util.Util;

public class RTask
{
	private int id;
	private int taskID;
	private String filePath;
	private String collectTime;
	private String stampTime;
	private String collectorName;
	private int readoptType;
	private int collectDegress;
	private int collectStatus;
	private String preStartTime;
	private String cause;

	public String getCause()
	{
		return this.cause;
	}

	public void setCause(String cause)
	{
		this.cause = cause;
	}

	public int getId()
	{
		return this.id;
	}

	public void setId(int id)
	{
		this.id = id;
	}

	public int getTaskID()
	{
		return this.taskID;
	}

	public void setTaskID(int taskID)
	{
		this.taskID = taskID;
	}

	public String getFilePath()
	{
		return this.filePath;
	}

	public void setFilePath(String filePath)
	{
		this.filePath = filePath;
	}

	public String getCollectTime()
	{
		return this.collectTime;
	}

	public void setCollectTime(String collectTime)
	{
		this.collectTime = collectTime;
	}

	public String getStampTime()
	{
		return this.stampTime;
	}

	public void setStampTime(String stampTime)
	{
		if (stampTime == null)
		{
			this.stampTime = Util.getDateString(new Date());
			return;
		}
		this.stampTime = stampTime;
	}

	public String getCollectorName()
	{
		return this.collectorName;
	}

	public void setCollectorName(String collectorName)
	{
		this.collectorName = collectorName;
	}

	public int getReadoptType()
  	{
		return this.readoptType;
  	}

	public void setReadoptType(int readoptType)
	{
		this.readoptType = readoptType;
	}

	public int getCollectDegress()
	{
		return this.collectDegress;
	}

	public void setCollectDegress(int collectDegress)
	{
		this.collectDegress = collectDegress;
	}

	public int getCollectStatus()
	{
		return this.collectStatus;
	}

	public void setCollectStatus(int collectStatus)
	{
		this.collectStatus = collectStatus;
	}

	public String getPreStartTime()
	{
		return this.preStartTime;
	}
	
	public void setPreStartTime(String preStartTime)
  	{
		this.preStartTime = preStartTime;
  	}
}