package com.turk.clusters.master;

import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.clusters.common.TaskObjInfo;
import com.turk.clusters.model.SlaveInfo;
import com.turk.clusters.model.TaskMsg;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;
import com.turk.delayprobe.DelayProbeMgr;

/**
 * 采集扫描线程
 * @author 
 *
 */
public class ScanThread
  implements Runnable
{
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
	    this.logger.info("开始扫描");
	    loadRegatherInfos();
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

	    		if (now.getTime() - lastScanTime.getTime() > 30000L)
	    		{
	    			DelayProbeMgr.time += 1;
	    			ScanInfo info = new ScanInfo();
	    			info.now = new Date(now.getTime());
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
	    			info.now = new Date(now.getTime());
	    			info.bReAdopt = true;

	    			synchronized (this.scanInfoQueue)
	    			{
	    				this.scanInfoQueue.add(info);
	    			}
	    			
	    			lastReAdoptTime = info.now;
	    		}

	    		try
	    		{
	    			//System.gc();
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
							if(slave.getStatus()!=1 && slave.getStatus()!=4)
								continue;
							
							//判断状态
							Date now = new Date();
							Date activetime = slave.getActiveTime();
							
							if(now.getTime() - activetime.getTime() > 5*60*1000L)
							{
								//超过5分钟没有报活修改状态
								if(slave.getStatus() == 4)
								{
									slave.setStatus(2);
									continue;
								}
								
								slave.setStatus(2);
								
								//选取最优的节点分派任务
			  					SlaveInfo slavenew = TaskManage.getInstance().getExecuteSlave();
			  					
			  					if(slavenew == null)
			  						continue;
			  					//构建任务 将该节点未完成的任务发到其他节点执行
			  					for(TaskMsg task : slave.getTaskMaps().values())
			  					{
				  					int taskID = task.getTaskID();
				  					TaskObjInfo infonew = new TaskObjInfo(taskID,slavenew.getServer(),slavenew.getPort());
				  					infonew.buildObj(task.getObjMap());
				  					infonew.SendTask();
				  					String strMsg = String.format("节点[%s]故障，30分钟未响应，节点标记为宕机", 
				  							slave.getServer(),task.getTaskID(),slavenew.getServer());
				  					logger.debug(strMsg);
			  					}
							}
							
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
								case 4: 
									this.logger.debug(String.format("Slave:[%s] Running! Last updatetime[%s]", slave.getServer(),slave.getActiveTime()));
									break;
								default:
									break;
							}
						}
					}
				}
				Thread.sleep(1000L);
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
	 * 正常采集
	 * @param now
	 * @return
	 */
	private boolean loadGatherInfos(Date now)
	{
		boolean bReturn = taskMgr.loadNormalTasksFromDB(now);

		int activeTaskCount = taskMgr.size();
		this.logger.info(this + ": 当前有效任务数为:" + activeTaskCount);

		return bReturn;
	}
	
	

	/**
	 * 补采
	 */
	private void loadRegatherInfos()
	{
		taskMgr.loadReGatherTasksFromDB();
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

class ScanInfo
{
	Date now;
	boolean bReAdopt;
}