package com.turk.framework;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.delayprobe.DelayProbeMgr;
import com.turk.task.TaskMgr;
import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class PersistentScanThread implements Runnable{
	private boolean stopFlag = false;

	private ArrayList<ScanInfo> scanInfoQueue = new ArrayList<ScanInfo>();

	private Thread thread = new Thread(this, toString());
	private ScanEndAction endAction;
	private Logger logger = LogMgr.getInstance().getSystemLogger();

	private static TaskMgr taskMgr = TaskMgr.getInstance();
	private static PersistentScanThread instance;

	public static synchronized PersistentScanThread getInstance()
	{
		if (instance == null)
    	{
			instance = new PersistentScanThread();
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
	    this.logger.info("��ʼɨ������Բɼ�����");
	    this.thread.start();
	    Date beginTime = new Date();
	    Date lastScanTime = beginTime;
	    Date lastReAdoptTime = beginTime;

	    while (!isStop())
	    {
	    	try
	    	{
	    		Date now = new Date();

	    		if (now.getTime() - lastScanTime.getTime() > 10000L)
	    		{
	    			DelayProbeMgr.time += 1;
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
	    		this.logger.error(this + "-startSystem: ɨ����������쳣.", e);
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
					
					
					this.logger.info(this + ": Current Active Thread Count:" + 
							Thread.activeCount());

					Util.showOSState();
						
					loadPersistentGatherInfos(info.now);

         		}

				Thread.sleep(1000*60*60L); //�߳�1Сʱ����һ��
			}
			catch (InterruptedException ie)
			{
				this.logger.warn("�����ɨ���̱߳����ǿ���ж�.");
				this.stopFlag = true;
				this.scanInfoQueue.clear();
				break;
			}
			catch (Exception e)
			{
				this.logger.error(this + ": ɨ���쳣.ԭ��:", e);
				break;
			}
		}
		if (this.endAction != null)
		{
			this.endAction.actionPerformed(taskMgr);
		}
		this.logger.info("ɨ������");
	}

	
	
	/**
	 * �����Բɼ�
	 * @param now
	 * @return
	 */
	private boolean loadPersistentGatherInfos(Date now)
	{
		boolean bReturn = taskMgr.loadPersistentTasksFromDB(now);

		int activeTaskCount = taskMgr.size();
		this.logger.info(this + ": ��ǰ��Ч������Ϊ:" + activeTaskCount);
		
		if (activeTaskCount == 0)
		{
			try
			{
				File f = new File("end.txt");
				f.createNewFile();
			}
			catch (Exception e)
			{
				this.logger.error(this + ": ����end.txt�ļ��쳣.ԭ��:", e);
			}
		}

		return bReturn;
	}

	public String toString()
	{
		return "Persistent Scan-Thread";
	}

	public static abstract interface ScanEndAction
	{
		public abstract void actionPerformed(TaskMgr paramTaskMgr);
	}
}
