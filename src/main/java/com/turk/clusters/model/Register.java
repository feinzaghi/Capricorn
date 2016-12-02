package com.turk.clusters.model;


import net.sf.json.JSONObject;

/**
 * �ڵ� ע��/���� ��Ϣ
 * @author Administrator
 *
 */
public class Register extends AbstractMsg{
	
	private String _server = "";
	private int _port = 0;
	private int _maxactivetask;  //�ڵ��������е����������
	private int _maxcltcount; //�ڵ����ɼ��߳���
	private int _currentcltcount;//��ǰ�ڵ�ɼ�������
	private int _flag = 1;
	
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
	
	/**
	 * �ڵ��������е����������
	 * @param maxactivetask
	 */
	public void setMaxActiveTask(int maxactivetask)
	{
		this._maxactivetask = maxactivetask;
	}
	
	/**
	 * �ڵ��������е����������
	 * @return
	 */
	public int getMaxActiveTask()
	{
		return this._maxactivetask;
	}
	
	public void setMaxCltCount(int maxcltcount)
	{
		this._maxcltcount = maxcltcount;
	}
	
	public int getMaxCltCount()
	{
		return this._maxcltcount;
	}
	
	public void setCurrentCltCount(int currentcltcount)
	{
		this._currentcltcount = currentcltcount;
	}
	
	public int getCurrentCltCount()
	{
		return this._currentcltcount;
	}
	
	public void setFlag(int flag)
	{
		this._flag = flag;
	}
	
	public int getFlag()
	{
		return this._flag;
	}
	
	public Register getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		Register register = null;
		register = (Register)JSONObject.toBean(jsonobject,
        		Register.class);
		
		return register;
	}
}
