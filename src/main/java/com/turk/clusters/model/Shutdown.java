package com.turk.clusters.model;

import net.sf.json.JSONObject;

/**
 * �ر�֪ͨ��Ϣ
 * @author Administrator
 *
 */
public class Shutdown extends AbstractMsg{
	
	private String _status;
	
	public void setStatus(String status)
	{
		this._status = status;
	}
	
	public String getStatus()
	{
		return this._status;
	}
	
	public Shutdown getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		Shutdown register = null;
		register = (Shutdown)JSONObject.toBean(jsonobject,
				Shutdown.class);
		
		return register;
	}
}
