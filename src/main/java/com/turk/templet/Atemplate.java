package com.turk.templet;

import java.util.ArrayList;

import com.turk.parser.Parser;

import com.turk.task.CollectObjInfo;

public abstract class Atemplate
{
	protected CollectObjInfo m_TaskInfo;
	protected Parser m_ParseData = null;
	private ArrayList<String> filelist;

	protected void parse()
	{
	}

	public ArrayList<String> getFilelist()
	{
		return this.filelist;
	}

	public void setFilelist(ArrayList<String> filelist)
	{
		this.filelist = filelist;
	}

	public CollectObjInfo getM_TaskInfo()
	{
		return this.m_TaskInfo;
 	}

	public void setM_TaskInfo(CollectObjInfo taskInfo)
	{
		this.m_TaskInfo = taskInfo;
	}

	public Parser getM_ParseData()
	{
		return this.m_ParseData;
	}

	public void setM_ParseData(Parser parseData)
	{
		this.m_ParseData = parseData;
	}
}