package com.turk.access;

import com.turk.collect.SocketServer;
import com.turk.util.Task;

public class SocketServerAccessor extends AbstractAccessor{
	
	SocketServer socket = null;
	
	public SocketServerAccessor()
	{
		
	}

	@Override
	public boolean access() throws Exception {
		// TODO Auto-generated method stub
		this.log.debug(this.name + ": 开始通过Socket连接方式处理字符流.");
		return this.parser.parseData();
		//return true;
	}

	@Override
	public void configure() throws Exception {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String info() {
		// TODO Auto-generated method stub
		String taskinfo = String.format("ID:%s Name:%s StartTime:%s", 
				this.taskInfo.getTaskID(),
				this.taskInfo.getDescribe(),this.getBeginExceuteTime());
		return taskinfo;
	}

	@Override
	protected boolean needExecuteImmediate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Task taskCore() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean useDb() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stopTask() {
		// TODO Auto-generated method stub
		log.debug("Accessor-socket stop");
		this.parser.Stop();
	}

}
