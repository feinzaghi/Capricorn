package com.turk.console.commands;


import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.turk.alarm.AlarmMgr;
import com.turk.alarm.ProcessStatus;
import com.turk.clusters.master.ScanThread;
import com.turk.clusters.master.TaskManage;
import com.turk.clusters.model.Shutdown;
import com.turk.clusters.model.SlaveInfo;
import com.turk.config.SystemConfig;
import com.turk.console.ConsoleMgr;
import com.turk.console.common.console.io.CommandIO;
import com.turk.datalog.DataLogMgr;
import com.turk.socket.Client;
import com.turk.app.appinterface;
import com.turk.db.GPDBPool;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.ThreadPool;
import com.turk.framework.DataLifecycleMgr;

public class MasterCommand extends BasicCommand{

	
	//���з�
	private String lineSeparator = "\r\n";

	private Logger log = LogMgr.getInstance().getAppLogger("master");
		
		
	@Override
	public boolean doCommand(String[] args,
			CommandIO io) throws Exception {
		// TODO Auto-generated method stub

		if ((args == null) || (args.length < 1))
		{
			io.println("syntax errors. input help /? for Command Help");
			return true;
		}
		
		if(args[0].trim().toLowerCase().equals("-a"))
		{
			StringBuffer sb = new StringBuffer();
			for(SlaveInfo slave:TaskManage.getInstance().getSlaves().values())
			{
				for(String task:slave.getTaskMaps().keySet())
				{
					sb.append(slave.getServer() + ":" + task + lineSeparator);
				}
				
			}
			io.println(sb.toString());
		}
		else if(args[0].trim().toLowerCase().equals("-server"))
		{
			if(args.length<2)
				return false;
			String server = args[1].trim().toLowerCase();
			StringBuffer sb = new StringBuffer();
			for(SlaveInfo slave:TaskManage.getInstance().getSlaves().values())
			{
				if(slave.getServer().equals(server))
				{
					for(String task:slave.getTaskMaps().keySet())
					{
						sb.append(slave.getServer() + ":" + task + lineSeparator);
					}
				}
				
			}
			io.println(sb.toString());
		}
		else if(args[0].trim().toLowerCase().equals("-l"))
		{
			StringBuffer sb = new StringBuffer();
			for(SlaveInfo slave:TaskManage.getInstance().getSlaves().values())
			{
				String strstatus = "";
				switch(slave.getStatus())
				{
					case 0:
						strstatus = "offline";
						break;
					case 1:
						strstatus = "online";
						break;
					case 2:
						strstatus = "timeout";
						break;
					case 3:
						strstatus = "interrupt";
						break;
					case 4:
						strstatus = "online-only-self";
						break;
				}
				String str = String.format("Server[%s]-[%s]:current task:%d"
						, slave.getServer(), strstatus, slave.getCurrentCltCount());
				sb.append(str + lineSeparator);
			}
			io.println(sb.toString());
		}
		else if(args[0].trim().toLowerCase().equals("-stop"))
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
		else if(args[0].trim().toLowerCase().equals("-stopslave"))
		{
			try
			{
				if(args.length < 2)
				{
					io.println("syntax errors. input help /? for Command Help");
					return false;
				}
				String IP = args[1].trim();
				SlaveInfo slave = TaskManage.getInstance().getSlaves().get(IP);
				if(slave == null)
				{
					io.println("��������Ľڵ�IP��ַ!");
					return false;
				}
				
				io.println("֪ͨ�ڵ�ֹͣ����....");
				
				Shutdown shut = new Shutdown();
				shut.setMsgID(9999); //
				shut.setStatus("STOP");
				
				io.println("�޸Ľڵ�״̬.....");
				slave.setCurrentCltCount(0);
				
				JSONObject jsonObject = JSONObject.fromObject(shut);
				
				Client clt = new Client(slave.getServer(),
			    		slave.getPort());
			    String Result = clt.SendMsg(jsonObject.toString());
			    io.print("Server["+ slave.getServer() +"]Stop...");
			    if(Result.equals("Done"))
			    {
			    	io.println("OK!");
			    	log.debug("MSGID-9999,SHUTDOWN-OK--"+slave.getServer());
			    }
			    else
			    {
			    	io.println(" Failure!" + Result);
			    	log.debug("MSGID-9999,SHUTDOWN-ERROR--"+slave.getServer() + " result:" + Result);
			    }
			}
			catch(Exception ex)
			{
				log.error("�رսڵ�����쳣",ex);
				io.println("Failure!" + ex.getMessage());
			}
			
		}
		else if(args[0].trim().toLowerCase().equals("-k"))
		{
			
		}
		return true;
	}
	
	private void awaitStop(final CommandIO printer)
	{
		ScanThread scanThread = ScanThread.getInstance();
		scanThread.setEndAction(new ScanThread.ScanEndAction()
		{
			public void actionPerformed(TaskManage taskMgr)
			{
				TaskManage.getInstance().StopSendTask();
				printer.println("ֹͣ������--ok");
				ThreadPool.getInstance().stoptask();//ֹͣ��ʵ��stop����������

				printer.println("֪ͨ�ڵ�ֹͣ����....");
				
				Shutdown shut = new Shutdown();
				shut.setMsgID(9999); //
				shut.setStatus("STOP");
				
				JSONObject jsonObject = JSONObject.fromObject(shut);

				for(SlaveInfo slave:TaskManage.getInstance().getSlaves().values())
				{
					Client clt = new Client(slave.getServer(),
				    		slave.getPort());
				    String Result = clt.SendMsg(jsonObject.toString());
				    printer.print("Server["+ slave.getServer() +"]Stop...");
				    if(Result.equals("Done"))
				    {
				    	printer.println("OK!");
				    	log.debug("MSGID-9999,SHUTDOWN-OK--"+slave.getServer());
				    }
				    else
				    {
				    	printer.println(" Failure!" + Result);
				    	log.debug("MSGID-9999,SHUTDOWN-ERROR--"+slave.getServer() + " result:" + Result);
				    }
				    slave.setCurrentCltCount(0);
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
				ConsoleMgr.getInstance(9020).stop();
				printer.println("exit!");
				System.exit(0);
			}
		});
		scanThread.stopScan();
	}

}
