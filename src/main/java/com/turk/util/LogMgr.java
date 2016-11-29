package com.turk.util;

import java.io.File;

import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class LogMgr
{
	private static LogMgr instance = null;
	private Logger systemLog;
	private Logger errorLog;
	//private static final String SYSTEMFILE = "../conf/log4j.properties";
	private static String SYSTEMFILE = ".." + File.separator + "config" + 
	  File.separator + "log4j.properties";
	
	private LogMgr()
	{
		try
		{
			System.out.println(System.getProperties().getProperty("JAVALIB"));
			SYSTEMFILE = System.getProperty("JAVALIB")
					+ File.separator + "conf" + 
					  File.separator + "log4j.properties";
			//File f = new File(SYSTEMFILE);
			//Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(f);
			//Element log4jEl = (Element)doc.getDocumentElement().getElementsByTagName("log4j").item(0);
			//log4jEl = (Element)log4jEl.getElementsByTagName("log4j:configuration").item(0);
			//DOMConfigurator.configure(log4jEl);
			System.out.println(SYSTEMFILE);
			PropertyConfigurator.configure(SYSTEMFILE);
			this.systemLog = Logger.getLogger("system");
			this.errorLog = Logger.getLogger("error");
			
		}
		catch (Exception e)
		{
			System.err.println("配置log4j时发生异常请检 log4 文件" + e.getMessage());
			e.printStackTrace();
		}
	}

	public static synchronized LogMgr getInstance()
	{
		if (instance == null)
		{
			instance = new LogMgr();
		}

		return instance;
	}

	/**
	 * 系统日志 全
	 * @return
	 */
	public Logger getSystemLogger()
	{
		return this.systemLog;
	}
	
	/**
	 * 错误日志
	 * @return
	 */
	public Logger getErrorLogger()
	{
		return this.errorLog;
	}

	public DBLogger getDBLogger()
	{
		return DBLogger.getInstance();
	}
	
	public Logger getAppLogger(String appname)
	{
		return Logger.getLogger(appname);
	}
}