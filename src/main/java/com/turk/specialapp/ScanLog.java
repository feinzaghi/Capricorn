package com.turk.specialapp;

import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.util.LogMgr;

/**
 * 通过扫描相关日志表，提供一些特别功能的标识
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
				{//开启话单idx文件上传
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
