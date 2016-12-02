package com.turk.specialapp.taurus.utele;

import net.sf.json.JSONObject;

/**
 * 客户端请求消息（WEB）
 * @author Administrator
 *
 */
public class RequestMsg extends Abstractmessage{

	private String _server = "";
	private int _port = 0;
	private String _key;
	private String _value;
	
	
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
	
	public void setKey(String key)
	{
		this._key = key;
	}
	
	public String getKey()
	{
		return this._key;
	}
	
	public void setValue(String value)
	{
		this._value = value;
	}
	
	public String getValue()
	{
		return this._value;
	}
	
	
	public RequestMsg getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		RequestMsg register = null;
		register = (RequestMsg)JSONObject.toBean(jsonobject,
				RequestMsg.class);
		
		return register;
	}
}
