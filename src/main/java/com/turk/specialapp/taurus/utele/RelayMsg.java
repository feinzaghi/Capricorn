package com.turk.specialapp.taurus.utele;

import net.sf.json.JSONObject;

public class RelayMsg extends Abstractmessage{
	
	private int _queueNum = 0;
	
	private String _status = "";

	/**
	 * 消息队列编号，在加入到队列之前分配
	 * @param queuenum
	 */
	public void setQueueNum(int queuenum)
	{
		this._queueNum = queuenum;
	}
	
	/**
	 * 消息队列编号，在加入到队列之前分配
	 * @return
	 */
	public int getQueueNum()
	{
		return this._queueNum;
	}
	
	public void setStatus(String status)
	{
		this._status = status;
	}
	
	public String getStatus()
	{
		return this._status;
	}
	
	
	public RelayMsg getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		RelayMsg register = null;
		register = (RelayMsg)JSONObject.toBean(jsonobject,
				RelayMsg.class);
		
		return register;
	}
}
