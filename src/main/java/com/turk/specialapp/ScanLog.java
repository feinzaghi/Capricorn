package com.turk.specialapp;

import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.util.LogMgr;

/**
 * ͨ��ɨ�������־���ṩһЩ�ر��ܵı�ʶ
 * @author Administrator
 *
 */
public class ScanLog implements Runnable{
	
	private Logger logger = LogMgr.getInstance().getSystemLogger();
	
	private Thread thread = new Thread(this, toString());
	
	private boolean stopFlag = false;
	
	private static ScanLog instance;
	
	public static synchronized ScanLog getInstance()
	{
		if (instance == null)
    	{
			instance = new ScanLog();
    	}
		return instance;
	}
	
	public synchronized boolean isStop()
	{
		return this.stopFlag;
	}
	
	public void startScan()
	{
	    this.logger.info("Start SacnLog");
	    this.thread.start();
	}
	
	public void stopScan()
	{
	    this.stopFlag = true;
	    this.thread.interrupt();
	}

	public void run()
	{
		while (!isStop())
		{
			try
			{
			
				if(SystemConfig.getInstance().CdrIndex())
				{//��������idx�ļ��ϴ�
					CdrDayComplate.getInstance().ExcuteScan();
				}
				
				
				
				Thread.sleep(10*60000L);
			}
			catch(Exception ex)
			{
				this.logger.error(ex);
			}
		}
				
		this.logger.info("End SacnLog");
	}

}
