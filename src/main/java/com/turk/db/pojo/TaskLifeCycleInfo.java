package com.turk.db.pojo;

import java.util.ArrayList;
import java.util.List;

public class TaskLifeCycleInfo extends Task
{
	private String costTime;
	private String dataTime;
	private List<RtaskLifeCycleInfo> reclts = new ArrayList<RtaskLifeCycleInfo>();
	private int recltsCount;

	public String getCostTime()
	{
		return this.costTime;
	}

	public void setCostTime(String costTime)
	{
		this.costTime = costTime;
	}

	public String getDataTime()
	{
		return this.dataTime;
	}

	public void setDataTime(String dataTime)
	{
		this.dataTime = dataTime;
	}
	
	public List<RtaskLifeCycleInfo> getReclts()
	{
		return this.reclts;
	}

	public int getRecltsCount()
	{
		this.recltsCount = this.reclts.size();
		return this.recltsCount;
	}
}