package com.turk.console.commands;

import com.turk.alarm.AlarmMgr;
import com.turk.alarm.ProcessStatus;
import com.turk.console.ConsoleMgr;
import com.turk.console.common.console.io.CommandIO;
import com.turk.datalog.DataLogMgr;
import com.turk.task.TaskMgr;

import com.turk.framework.DataLifecycleMgr;
import com.turk.framework.ScanThread;
import com.turk.Config.SystemConfig;
import com.turk.app.appinterface;
import com.turk.db.GPDBPool;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.ThreadPool;

/**
 * �������еĳ���
 * @author Administrator
 *
 */
public class StopCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		String i = null;
		int taskCount = TaskMgr.getInstance().size();
    	if ((args != null) && (args.length > 0))
    	{
    		i = args[0].trim();
    		i = i.equalsIgnoreCase("-i") ? i : null;
    	}

    	if (i != null)
    	{
    		if (taskCount == 0)
    			System.exit(0);
    		else {
    			System.exit(-1);
    		}
    	}
    	else if ((args == null) || (args.length < 1))
    	{
    		String strLine = io.readLine("��ǰ��" + taskCount + 
    			"������������,�Ƿ�Ҫ�����˳�? [y|n]  ");

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
    	}

    	return true;
    }

	private void awaitStop(final CommandIO printer)
	{
		ScanThread scanThread = ScanThread.getInstance();
		scanThread.setEndAction(new ScanThread.ScanEndAction()
		{
			public void actionPerformed(TaskMgr taskMgr)
			{
				/*
				List<CollectObjInfo> tasks = taskMgr.list();
				for (CollectObjInfo task : tasks)
				{
					String taskType = (task instanceof RegatherObjInfo) ? "��������" : "�ɼ�����";
					Task th = task.getCollectThread();
					
					int id = task.getKeyID();
					printer.print("���ڵȴ�" + taskType + "(id:" + id + ")����");
					//try
					//{
					//	th.join();
					//}
					//catch (InterruptedException e)
					//{
					//	e.printStackTrace();
					//}
					//th.interrupt();
					printer.println("......�ѽ���");
				}*/
				TaskMgr.getInstance().StopSendTask();
				ThreadPool.getInstance().stoptask();//ֹͣ��ʵ��stop����������
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
			}
		});
		scanThread.stopScan();
	}
}