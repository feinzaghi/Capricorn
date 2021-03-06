package com.turk.clusters.master;

import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.clusters.model.SlaveInfo;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class StatScanThread implements Runnable{
	private boolean stopFlag = false;

	private ArrayList<ScanInfo> scanInfoQueue = new ArrayList<ScanInfo>();

	private Thread thread = new Thread(this, toString());
	private ScanEndAction endAction;
	private Logger logger = LogMgr.getInstance().getAppLogger("master");

	private static TaskManage taskMgr = TaskManage.getInstance();
	private static ScanThread instance;

	public static synchronized ScanThread getInstance()
	{
		if (instance == null)
    	{
			instance = new ScanThread();
    	}
		return instance;
	}

	public synchronized boolean isStop()
	{
		return this.stopFlag;
	}

	public void stopScan()
	{
	    this.stopFlag = true;
	    this.thread.interrupt();
	    this.scanInfoQueue.clear();
	}

	public void startScan()
	{
	    this.logger.info("开始扫描汇总任务");
	    //loadRegatherInfos();
	    this.thread.start();
	
	    Date beginTime = new Date();
	    Date lastScanTime = beginTime;
	    Date lastReAdoptTime = beginTime;

	    while (!isStop())
	    {
	    	try
	    	{
	    		if(!TaskManage.getInstance().IsSlaves())
	    		{
	    			Thread.sleep(1000);
	    			continue;
	    		}
	    		
	    		Date now = new Date();

	    		if (now.getTime() - lastScanTime.getTime() > 10000L)
	    		{
	    			//DelayProbeMgr.time += 1;
	    			ScanInfo info = new ScanInfo();
	    			info.now = new Date(lastScanTime.getTime() + 10000L);
	    			info.bReAdopt = false;

	    			synchronized (this.scanInfoQueue)
	    			{
	    				this.scanInfoQueue.add(info);
	    			}

	    			lastScanTime = info.now;
	    		}

	    		if (now.getTime() - lastReAdoptTime.getTime() > 300000L)
	    		{
	    			ScanInfo info = new ScanInfo();
	    			info.now = new Date(lastReAdoptTime.getTime() + 300000L);
	    			info.bReAdopt = true;

	    			synchronized (this.scanInfoQueue)
	    			{
	    				this.scanInfoQueue.add(info);
	    			}
	    			
	    			lastReAdoptTime = info.now;
	    		}

	    		try
	    		{
	    			Thread.sleep(1000L);
	    		}
	    		catch (InterruptedException e)
	    		{
	    			stopScan();
	    		}
	    	}
	    	catch (Exception e)
	    	{
	    		this.logger.error(this + "-startSystem: 扫描任务出现异常.", e);
	    	}

	    }

	    CommonDB.closeDbConnection();
	}

	public void setEndAction(ScanEndAction endAction)
	{
		this.endAction = endAction;
	}

	public void run()
	{
		while (!isStop())
		{
			try
			{
				if (this.scanInfoQueue.size() > 0)
				{
					ScanInfo info = null;
					synchronized (this.scanInfoQueue)
					{
						info = (ScanInfo)this.scanInfoQueue.remove(0);
					}
					
					if (info.bReAdopt)
					{
						loadRegatherInfos();
					}
					else
					{
						this.logger.info(this + ": Current Active Thread Count:" + 
								Thread.activeCount());

						Util.showOSState();
						
						loadGatherInfos(info.now);
						
						//扫描当前各节点状态信息
						for(SlaveInfo slave: TaskManage.getInstance().getSlaves().values())
						{
							switch(slave.getStatus())
							{
								case 1: 
									this.logger.debug(String.format("Slave:[%s] Running! Last updatetime[%s]", slave.getServer(),slave.getActiveTime()));
									break;
								case 2:
									this.logger.debug(String.format("Slave:[%s] Warning! Last updatetime[%s]", slave.getServer(),slave.getActiveTime()));
									break;
								case 3:
									this.logger.debug(String.format("Slave:[%s] Stop! Last updatetime[%s]", slave.getServer(),slave.getActiveTime()));
									break;
								default:
									break;
							}
						}
					}
				}
				Thread.sleep(10L);
			}
			catch (InterruptedException ie)
			{
				this.logger.warn("任务表扫描线程被外界强行中断.");
				this.stopFlag = true;
				this.scanInfoQueue.clear();
				break;
			}
			catch (Exception e)
			{
				this.logger.error(this + ": 扫描异常.原因:", e);
				break;
			}
		}
		if (this.endAction != null)
		{
			this.endAction.actionPerformed(taskMgr);
		}
		this.logger.info("扫描线束");
	}

	/**
	 * 正常汇总任务
	 * @param now
	 * @return
	 */
	private boolean loadGatherInfos(Date now)
	{
		boolean bReturn = taskMgr.loadStatTaskFromDB(now);

		//int activeTaskCount = taskMgr.size();
		//this.logger.info(this + ": 当前有效任务数为:" + activeTaskCount);
		
		//if (activeTaskCount == 0)
		//{
		//	try
		//	{
		//		File f = new File("end.txt");
		//		f.createNewFile();
		//	}
		//	catch (Exception e)
		//	{
		//		this.logger.error(this + ": 创建end.txt文件异常.原因:", e);
		//	}
		//}

		return bReturn;
	}
	
	

	/**
	 * 补采
	 */
	private void loadRegatherInfos()
	{
		//taskMgr.loadReGatherTasksFromDB();
	}

	public String toString()
	{
		return "Master-Scan-Thread";
	}

	public static abstract interface ScanEndAction
	{
		public abstract void actionPerformed(TaskManage paramTaskMgr);
	}
}
