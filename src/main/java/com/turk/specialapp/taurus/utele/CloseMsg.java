package com.turk.specialapp.taurus.utele;

import net.sf.json.JSONObject;

/**
 * Close Message
 * @author Administrator
 *
 */
public class CloseMsg extends Abstractmessage{
	
	private int _queueNum = 0;
	
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
	

	public CloseMsg getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		CloseMsg register = null;
		register = (CloseMsg)JSONObject.toBean(jsonobject,
				CloseMsg.class);
		
		return register;
	}
}
