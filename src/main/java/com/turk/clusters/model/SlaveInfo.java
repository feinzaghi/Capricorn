package com.turk.clusters.model;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * �����нڵ������
 * @author Administrator
 *
 */
public class SlaveInfo {
	
	private String _server; //������IP
	private int _port; //�˿�
	private Map<String,TaskMsg> _taskmaps = new HashMap<String, TaskMsg>(); //�ڵ����е����񼯺�
	private int _maxactivetask;  //�ڵ��������е����������
	private int _status; //״̬  1:����,2:��ʱδ��Ӧ,0:�쳣����,3:��Ϊ��ֹ
	private int _maxcltcount; //�ڵ����ɼ��߳���
	private int _currentcltcount;//��ǰ�ڵ�ɼ�������
	
	private Date _activetime; //���һ�α���ʱ��
	
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
	 * ״̬  1:����,2:��ʱδ��Ӧ,0:�쳣����,3:��Ϊ��ֹ
	 * @param status
	 */
	public void setStatus(int status)
	{
		this._status = status;
	}
	
	/**
	 * ״̬  1:����,2:��ʱδ��Ӧ,0:�쳣����,3:��Ϊ��ֹ,4:������ֻ����ָ������
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
	 * ��ǰ�ڵ����е�������
	 * @param currentcltcount
	 */
	public synchronized void setCurrentCltCount(int currentcltcount)
	{
		this._currentcltcount = currentcltcount;
	}
	
	/**
	 * ��ǰ�ڵ����е�������
	 * @return
	 */
	public int getCurrentCltCount()
	{
		return this._currentcltcount;
	}
}

