package com.turk.clusters.slave;

import org.apache.log4j.Logger;

import com.turk.clusters.model.StatTaskInfo;

import com.turk.util.LogMgr;

public class StatTaskExecute {

	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	/**
	 * ִ�вɼ�
	 * @param info
	 */
	public void Execute(StatTaskInfo info)
	{
		switch(info.getTaskType())
		{
			case 1://·�⣺
				log.debug("����·�����:"+info.getTaskName() + " Time:" + info.getStartTime());
				//����·������߳�
				//DTMain DTThread = DTMain.getInstance();
				//DTThread.RunTask(info.getFTPIP(),info.getFTPUser(),info.getFTPPwd(),
				//		info.getFTPPath());
				break;
			default:
				return;
		}
	}
}
