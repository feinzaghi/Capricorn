package com.turk.clusters.slave;

import org.apache.log4j.Logger;

import com.turk.alarm.AlarmMgr;
import com.turk.alarm.ProcessStatus;
import com.turk.app.appinterface;
import com.turk.clusters.common.IExecute;
import com.turk.config.SystemConfig;
import com.turk.console.ConsoleMgr;
import com.turk.datalog.DataLogMgr;
import com.turk.db.GPDBPool;
import com.turk.framework.DataLifecycleMgr;
import com.turk.rpc.SlaveNettyServerHandler;
import com.turk.task.TaskMgr;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.ThreadPool;

public class ShutdownExecute implements IExecute{

	private Logger log = Logger.getLogger(SlaveNettyServerHandler.class);
	
	@Override
	public String Execute(String msgBody) {
		// TODO Auto-generated method stub
	  	  shutdown();
	//	  pw.println("Done");//返回客户端信息
	//      pw.flush();
		  log.debug("Close console service.");
		  ConsoleMgr.getInstance(SystemConfig.getInstance().getCollectPort()).stop();
		  log.debug("exit!");
		  System.exit(0);
		  return "Done";
	}

	private void shutdown()
	  {
		  log.debug("receive shutdown command");
		  ThreadPool.getInstance().stoptask();//停止已实现stop方法的任务
		  SlaveActive.getInstance().Stop();
		  while(TaskMgr.getInstance().size() > 0)
		  {
			  try 
			  {
					Thread.sleep(10000L);
					log.debug("waiting for stop");
							
			  } catch (InterruptedException e) {
							// TODO Auto-generated catch block
					e.printStackTrace();
			  }
		  }
					
					log.debug("No task running,close thread pool.");

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
					log.debug("Close DB Pool.");
					CommonDB.closeDbConnection();
					DbPool.close();
					GPDBPool.close();
	  }
}
