package com.turk.clusters.model;

import java.util.HashMap;

import net.sf.json.JSONObject;

public class TaskMsg extends AbstractMsg{
	
	private int _taskid;
	
	public int getTaskID()
	{
		return this._taskid;
	}
	
	public void setTaskID(int taskid)
	{
		this._taskid = taskid;
	}
	
	
	private HashMap<String,String> _objMap;
	
	public HashMap<String,String> getObjMap()
	{
		return this._objMap;
	}
	
	public void setObjMap(HashMap<String,String> objmap)
	{
		this._objMap = objmap;
	}
	
	private int _isreclt;
	
	public void setIsReCLT(int isreclt)
	{
		this._isreclt = isreclt;
	}
	
	public int getIsReCLT()
	{
		return this._isreclt;
	}
	
	
	@SuppressWarnings("static-access")
	public TaskMsg getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		TaskMsg obj = null;
		obj = (TaskMsg)jsonobject.toBean(jsonobject,
				TaskMsg.class);
		return obj;
	}
}
