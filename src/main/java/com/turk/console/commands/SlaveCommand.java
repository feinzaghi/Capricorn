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
			
	    	String strLine = io.readLine("ȷ����Ҫֹͣ����? [y|n]  ");
		    		if ((strLine.equalsIgnoreCase("n")) || 
	    				(strLine.equalsIgnoreCase("no"))) {
	    			return true;
	    		}
	    		if ((!strLine.equalsIgnoreCase("n")) && 
	    				(!strLine.equalsIgnoreCase("no")) && 
	    				(!strLine.equalsIgnoreCase("y")) && 
	    				(!strLine.equalsIgnoreCase("yes")))
	    		{
	    			io.println("�Ƿ�����,����ֹͣ�ɼ�ϵͳ.");
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
		ThreadPool.getInstance().stoptask();//ֹͣ��ʵ��stop����������
			printer.println("֪ͨMasterֹͣ����....");
				
		if(!SlaveActive.getInstance().Stop())
		{
			String strLine = printer.readLine("Masterֹͣ�����쳣���޷��رսڵ�,�Ƿ�ǿ��ֹͣslave�ڵ�[y|n]");
			if ((!strLine.equalsIgnoreCase("n")) && 
    				(!strLine.equalsIgnoreCase("no")) && 
    				(!strLine.equalsIgnoreCase("y")) && 
    				(!strLine.equalsIgnoreCase("yes")))
    		{
				printer.println("�Ƿ�����,����ֹͣ�ɼ�ϵͳ.");
    			return;
    		}
		}
		else
		{
			printer.println("Master command -OK-");
		}
		if(TaskMgr.getInstance().size() > 0)
		{
			printer.println("��ǰ��" + TaskMgr.getInstance().size() +
					"������������,�ȴ�...");
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
				//����
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
