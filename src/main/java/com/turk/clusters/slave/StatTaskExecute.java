package com.turk.clusters.slave;

import org.apache.log4j.Logger;

import com.turk.clusters.common.IExecute;
import com.turk.clusters.model.StatTaskInfo;
import com.turk.util.LogMgr;

public class StatTaskExecute implements IExecute{

	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * 执行采集
	 * @param info
	 */
	public String Execute(String msgBody)
	{
		StatTaskInfo info = new StatTaskInfo();
		info = info.getByJson(msgBody);
    	if(info != null)
    	{
    		switch(info.getTaskType())
    		{
    			case 1://路测：
    				log.debug("启动路测汇总:"+info.getTaskName() + " Time:" + info.getStartTime());
    				//启动路测汇总线程
    				//DTMain DTThread = DTMain.getInstance();
    				//DTThread.RunTask(info.getFTPIP(),info.getFTPUser(),info.getFTPPwd(),
    				//		info.getFTPPath());
    				break;
    			default:
    				return "Done";
    		}
    	}
		
		
		return "Done";
	}
}
