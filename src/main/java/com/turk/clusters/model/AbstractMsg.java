package com.turk.clusters.model;

/**
 * 通信消息抽象类
 * @author Administrator
 *
 */
public abstract class AbstractMsg {

	protected int _msgid;
	
	public void setMsgID(int msgid)
	{
		this._msgid = msgid;
	}
	
	public int getMsgID()
	{
		return this._msgid;
	}
}
