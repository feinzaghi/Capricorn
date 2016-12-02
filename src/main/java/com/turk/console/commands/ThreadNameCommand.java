package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

public class ThreadNameCommand extends BasicCommand{

	@Override
	public boolean doCommand(String[] args, CommandIO io) throws Exception {
		ThreadGroup group = 
			Thread.currentThread().getThreadGroup();
		
		
		//创建一个目标数组
		int estimatedSize = group.activeCount() * 2;
		Thread[] slackList = new Thread[estimatedSize];
		
		//把线程组复制到指定的线程组中
		int actualSize = group.enumerate(slackList);
		
		// 复制到特定大小的线程组中
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
