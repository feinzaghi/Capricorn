package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

public class ThreadNameCommand extends BasicCommand{

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		ThreadGroup group = 
			Thread.currentThread().getThreadGroup();
		
		
		//����һ��Ŀ������
		int estimatedSize = group.activeCount() * 2;
		Thread[] slackList = new Thread[estimatedSize];
		
		//���߳��鸴�Ƶ�ָ�����߳�����
		int actualSize = group.enumerate(slackList);
		
		// ���Ƶ��ض���С���߳�����
		Thread[] list = new Thread[actualSize];
		System.arraycopy(slackList, 0, list, 0, actualSize);
		
		String threadName="";
		for(Thread thread : list){
			if(threadName=="")
				threadName+="active thread name: ["+thread.getName()+"]\r\n";
			else
				threadName+="   active thread name: ["+thread.getName()+"]\r\n";
		}
		 io.println(threadName);
		 return true;
	}

}
