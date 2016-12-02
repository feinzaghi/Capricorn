package com.turk.console.commands;

import java.io.IOException;

import com.turk.alarm.AlarmMgr;
import com.turk.alarm.ProcessStatus;
import com.turk.clusters.slave.SlaveActive;
import com.turk.console.ConsoleMgr;
import com.turk.console.common.console.io.CommandIO;
import com.turk.datalog.DataLogMgr;
import com.turk.task.TaskMgr;

import com.turk.Config.SystemConfig;
import com.turk.app.appinterface;
import com.turk.db.GPDBPool;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.ThreadPool;
import com.turk.framework.DataLifecycleMgr;

public class SlaveCommand extends BasicCommand{
	
	//private Logger log = LogMgr.getInstance().getAppLogger("slave");
			
			
	@Override
	public boolean doCommand(String[] args,
			CommandIO io) throws Exception {
		// TODO Auto-generated method stub
		if ((args == null) || (args.length < 1))
		{
			io.println("syntax errors. input help /? for Command Help");
			return true;
		}
		
		if(args[0].trim().toLowerCase().equals("-stop"))
		{
			
	    	String strLine = io.readLine("确认需要停止程序? [y|n]  ");
		    		if ((strLine.equalsIgnoreCase("n")) || 
	    				(strLine.equalsIgnoreCase("no"))) {
	    			return true;
	    		}
	    		if ((!strLine.equalsIgnoreCase("n")) && 
	    				(!strLine.equalsIgnoreCase("no")) && 
	    				(!strLine.equalsIgnoreCase("y")) && 
	    				(!strLine.equalsIgnoreCase("yes")))
	    		{
	    			io.println("非法输入,放弃停止采集系统.");
	    			return true;
	    		}
	    		awaitStop(io);
	    	
		    	return true;	
		}
		else if(args[0].trim().toLowerCase().equals("-k"))
		{
			
		}
		return true;
	}
	
	private void awaitStop(final CommandIO printer)
	{
		try {
		printer.println("Slave stop...");
		ThreadPool.getInstance().stoptask();//停止已实现stop方法的任务
			printer.println("通知Master停止服务....");
				
		if(!SlaveActive.getInstance().Stop())
		{
			String strLine = printer.readLine("Master停止服务异常，无法关闭节点,是否强制停止slave节点[y|n]");
			if ((!strLine.equalsIgnoreCase("n")) && 
    				(!strLine.equalsIgnoreCase("no")) && 
    				(!strLine.equalsIgnoreCase("y")) && 
    				(!strLine.equalsIgnoreCase("yes")))
    		{
				printer.println("非法输入,放弃停止采集系统.");
    			return;
    		}
		}
		else
		{
			printer.println("Master command -OK-");
		}
		if(TaskMgr.getInstance().size() > 0)
		{
			printer.println("当前有" + TaskMgr.getInstance().size() +
					"任务正在运行,等待...");
			printer.println(ThreadPool.getInstance().getRunTask());
		}
			while(TaskMgr.getInstance().size() > 0)
			{
				try 
				{
					printer.print(".");
					Thread.sleep(1000L);
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			printer.println("No task running,close thread pool.");
			ThreadPool.getInstance().destroy();
				if(SystemConfig.getInstance().isEnableRunDTStatistic())
			{
				//反射
				try
				{
					appinterface appdt = ((appinterface)Class.forName("DT.DTMain").newInstance()).getInstance();
					appdt.Shutdown();
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			
			ProcessStatus.getInstance().stopScan();
			
			DataLifecycleMgr.getInstance().stop();
			AlarmMgr.getInstance().shutdown();
			DataLogMgr.getInstance().commit();
			LogMgr.getInstance().getDBLogger().dispose();
			printer.println("Close DB Pool.");
			CommonDB.closeDbConnection();
			DbPool.close();
			GPDBPool.close();
			printer.println("Close console service.");
			ConsoleMgr.getInstance(SystemConfig.getInstance().getCollectPort()).stop();
			printer.println("exit!");
			System.exit(0);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
