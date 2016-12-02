package com.turk.db.pojo;

import java.util.Date;

/**
 * 采集日志实体类
 * @author Turk
 *
 */
public class LogCltInsert
{
	private int taskID;
	private int omcID;
	private String tbName;
	private Date stampTime;
	private Date vSysDate;
	private int count;
	private byte calFlag = 0;

	public int getTaskID()
	{
		return this.taskID;
	}

	public void setTaskID(int taskID)
	{
		this.taskID = taskID;
	}

	public int getOmcID()
	{
		return this.omcID;
	}

	public void setOmcID(int omcID)
	{
		this.omcID = omcID;
	}

	public String getTbName()
	{
		return this.tbName;
	}

	public void setTbName(String tbName)
	{
		this.tbName = tbName;
	}

	public Date getStampTime()
	{
		return this.stampTime;
	}

	public void setStampTime(Date stampTime)
	{
		this.stampTime = stampTime;
	}

	public Date getVSysDate()
	{
		return this.vSysDate;
	}

	public void setVSysDate(Date sysDate)
	{
		this.vSysDate = sysDate;
	}

	public int getCount()
	{
		return this.count;
	}

	public void setCount(int count)
	{
		this.count = count;
	}

	public byte getCalFlag()
	{	
		return this.calFlag;
	}

	public void setCalFlag(byte calFlag)
	{
		this.calFlag = calFlag;
	}
}