package com.turk.clusters.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 运行中节点的详情
 * @author Administrator
 *
 */
public class SlaveInfo {
	
	private String _server; //服务器IP
	private int _port; //端口
	private Map<String,TaskMsg> _taskmaps = new HashMap<String, TaskMsg>(); //节点运行的任务集合
	private int _maxactivetask;  //节点运行运行的最大活动任务数
	private int _status; //状态  1:正常,2:超时未响应,0:异常掉线,3:人为终止
	private int _maxcltcount; //节点最大采集线程数
	private int _currentcltcount;//当前节点采集任务数
	
	private Date _activetime; //最近一次报活时间
	
	public void setServer(String server)
	{
		this._server = server;
	}
	
	public String getServer()
	{
		return this._server;
	}
	
	public void setPort(int port)
	{
		this._port = port;
	}
	
	public int getPort()
	{
		return this._port;
	}
	
	public synchronized void setTaskMaps(String key,TaskMsg obj)
	{
		if(!_taskmaps.containsKey(key))
		{
			_taskmaps.put(key, obj);
		}
	}
	
	public Map<String,TaskMsg> getTaskMaps()
	{
		return this._taskmaps;
	}
	
	public synchronized TaskMsg deleteTask(String key)
	{
		return this._taskmaps.remove(key);
	}
	
	public void setMaxActiveTask(int maxactivetask)
	{
		this._maxactivetask = maxactivetask;
	}
	
	public int getMaxActiveTask()
	{
		return this._maxactivetask;
	}
	
	/**
	 * 状态  1:正常,2:超时未响应,0:异常掉线,3:人为终止
	 * @param status
	 */
	public void setStatus(int status)
	{
		this._status = status;
	}
	
	/**
	 * 状态  1:正常,2:超时未响应,0:异常掉线,3:人为终止,4:正常且只运行指定任务
	 * @return
	 */
	public int getStatus()
	{
		return this._status;
	}
	
	public void setActiveTime(Date activetime)
	{
		this._activetime = activetime;
	}
	
	public Date getActiveTime()
	{
		return this._activetime;
	}
	
	public void setMaxCltCount(int maxcltcount)
	{
		this._maxcltcount = maxcltcount;
	}
	
	public int getMaxCltCount()
	{
		return this._maxcltcount;
	}
	
	/**
	 * 当前节点运行的任务数
	 * @param currentcltcount
	 */
	public synchronized void setCurrentCltCount(int currentcltcount)
	{
		this._currentcltcount = currentcltcount;
	}
	
	/**
	 * 当前节点运行的任务数
	 * @return
	 */
	public int getCurrentCltCount()
	{
		return this._currentcltcount;
	}
}

