package com.turk.db.pojo;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Date;

import com.turk.db.dao.TaskDAO;

import com.turk.task.RegatherObjInfo;
import com.turk.task.RegatherStatistics;
import com.turk.util.Util;

public class RtaskLifeCycleInfo extends RTask
{
	private String costTime = "";
	private String recltType;
	private String recltStatus;
	private String shortPath;
	private String startTime;

	public RtaskLifeCycleInfo(RTask rtask)
	{
		setId(rtask.getId());
		setTaskID(rtask.getTaskID());
		setFilePath(rtask.getFilePath().replace("\n", "").replace("\r", ""));
		setCollectTime(rtask.getCollectTime());
		setStampTime(rtask.getStampTime());
		setReadoptType(rtask.getReadoptType());
		setCollectStatus(rtask.getCollectStatus());
		setCause(rtask.getCause());
	}

	public RtaskLifeCycleInfo()
	{
	}

	public String getCostTime()
	{
		return this.costTime;
	}

	public void setCostTime(String costTime)
	{
		this.costTime = costTime;
	}

	public String getRecltType()
	{
		if (getReadoptType() == 0)
		{
			this.recltType = "�Զ����";
		}
		else
		{
			this.recltType = "�ֶ����";
		}
		return this.recltType;
	}

	public String getRecltStatus()
	{
		switch (getCollectStatus())
		{
			case 0:
				this.recltStatus = "������";
				break;
			case -1:
				this.recltStatus = "�Ѵ���󲹲ɴ���";
				break;
			case 3:
				this.recltStatus = "�����";
				break;
			case 1:
			case 2:
			default:
				this.recltStatus = ("����(" + getCollectStatus() + ")");
		}

		return this.recltStatus;
	}

	public String getShortPath()
	{
		if (Util.isNotNull(getFilePath()))
		{
			if (getFilePath().length() > 33)
			{
				this.shortPath = (getFilePath().substring(0, 30) + "...");
			}
			else
			{
				this.shortPath = getFilePath();
			}
		}
		else
		{
			this.shortPath = "";
		}
		return this.shortPath;
  	}

	public String getRcltTimes()
	{
		if (getCostTime().equals("δ����")) 
			return "δ����";
		if (getCollectStatus() == 3) 
			return getRecltStatus();
		RegatherObjInfo rinfo = new RegatherObjInfo(getId() + 10000000, getTaskID());
		rinfo.setCollectPath(getFilePath());
		try
		{
			rinfo.setLastCollectTime(new Timestamp(Util.getDate1(getCollectTime()).getTime()));
		}
	    catch (ParseException localParseException)
	    {
	    }
	    int recltTimes = RegatherStatistics.getInstance().getRecltTimes(rinfo);

	    return "��" + (recltTimes - 1) + "��";
	}

	public String getStartTime()
	{
		RegatherObjInfo rinfo = new RegatherObjInfo(getId() + 10000000, getTaskID());
		rinfo.setCollectPath(getFilePath());
		try
		{
			rinfo.setLastCollectTime(new Timestamp(Util.getDate1(getCollectTime()).getTime()));
		}
		catch (ParseException localParseException)
		{
		}
		int recltTimes = RegatherStatistics.getInstance().getRecltTimes(rinfo);
		try
		{
			long time = Util.getDate1(getStampTime()).getTime() + 
				new TaskDAO().getById(getTaskID()).getRedoTimeOffset() * 
				recltTimes * 60 * 1000;
			this.startTime = Util.getDateString(new Date(time));
		}
		catch (ParseException localParseException1)
		{
		}
		return this.startTime;
	}
}