package com.turk.config;

import java.io.File;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.turk.des.DESDecryptor;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.PropertiesXML;
import com.turk.util.Util;

/**
 * Copyright (C) 2011 UTL
 * ��Ȩ���С� 
 *
 * �ļ�����SystemConfig.java
 * �ļ�����������ϵͳ����
 * 
 * �������ڣ�
 *
 * �޸����ڣ�
 * �޸�������
 *
 * �޸����ڣ�
 * �޸�������
 */
public class SystemConfig
{
	private PropertiesXML propertiesXML;
	
	/**
	 * �����ļ�
	 */
	private static String SYSTEMFILE = "." + File.separator + "config" + 
    	File.separator + "config.xml";
  //private static final String SYSTEMFILE = "C:\\config.xml";
  

	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private static SystemConfig Instance = null;
	private String realDbUser;
	private String realDbPwd;

	private SystemConfig()
    	throws SystemConfigException
    {
		SYSTEMFILE = "." + File.separator + "config" + 
		    	File.separator + "config.xml";
		this.propertiesXML = new PropertiesXML(SYSTEMFILE);

    }

	public static SystemConfig getInstance()
	{
		if (Instance == null)
		{
			try
			{
				Instance = new SystemConfig();
			}
			catch (SystemConfigException e)
			{
				logger.error("����SystemConfig����ʱ�����쳣", e);
				return null;
			}
		}
		return Instance;
	}

	public static void setInstance(SystemConfig instance)
	{
		Instance = instance;
	}

	public String getPoolName()
	{
		String dbName = this.propertiesXML.getProperty("config.db.name");

		if (Util.isNull(dbName))
		{
			dbName = "DC_POOL";
		}
		return dbName;
	}
  
	/**
	 * ��ȡ�Ƿ���й���ɼ��������ļ�
	 * @return
	 */
	public boolean isShare(){
	  	boolean b = false;
	    String str = this.propertiesXML.getProperty("config.share.enable");
	    if (Util.isNull(str)) return b;
	    str = str.toLowerCase().trim();
	    if ((str.equals("on")) || (str.equals("true")))
	    {
	      b = true;
	    }
	    else if ((str.equals("off")) || (str.equals("false")))
	    {
	      b = false;
	    }
	    return b;
	}
  
	/**
	 * ��ȡ����ƽ̨�����Ƿ��ϴ�ftp����
	 * @return
	 */
	public boolean isFtp(){
	  	boolean b = false;
	    String str = this.propertiesXML.getProperty("config.share.ftp.enable");
	    if (Util.isNull(str)) return b;
	    str = str.toLowerCase().trim();
	    if ((str.equals("on")) || (str.equals("true")))
	    {
	      b = true;
	    }
	    else if ((str.equals("off")) || (str.equals("false")))
	    {
	      b = false;
	    }
	    return b;
	}
	
	/**
	 * 
	 * @return
	 */
	public boolean CdrIndex(){
	  	boolean b = false;
	    String str = this.propertiesXML.getProperty("config.specialapp.cdrindex");
	    if (Util.isNull(str)) return b;
	    str = str.toLowerCase().trim();
	    if ((str.equals("on")) || (str.equals("true")))
	    {
	      b = true;
	    }
	    else if ((str.equals("off")) || (str.equals("false")))
	    {
	      b = false;
	    }
	    return b;
	}
	
	public boolean IsStartTaurusSocket(){
	  	boolean b = false;
	    String str = this.propertiesXML.getProperty("config.specialapp.taurussocket");
	    if (Util.isNull(str)) return b;
	    str = str.toLowerCase().trim();
	    if ((str.equals("on")) || (str.equals("true")))
	    {
	      b = true;
	    }
	    else if ((str.equals("off")) || (str.equals("false")))
	    {
	      b = false;
	    }
	    return b;
	}
	
	/**
	 * ����ƽ̨��ʼ�ɼ�ʱ��
	 * @return
	 */
	public String getShareName()
	{
		String lastTime = this.propertiesXML.getProperty("config.share.lastTime");
		if(Util.isNull(lastTime))
			lastTime="";
		return lastTime;
	}
  
	/**
	 * ����ƽ̨�ϴ�ftp IP����
	 * @return
	 */
	public String getIp()
	{
		String ip = this.propertiesXML.getProperty("config.share.ftp.ip");
		if(Util.isNull(ip)){
			ip="";
		}
		return ip;
	}
	
	/**
	 * ����ƽ̨�ϴ�ftp �˿�����
	 * @return
	 */
	public String getPort()
	{
		String port = this.propertiesXML.getProperty("config.share.ftp.port");
		if(Util.isNull(port)){
			port="";
		}
		return port;
	}
  
	/**
	 * ����ƽ̨�ϴ�ftp �û�����
	 * @return
	 */
	public String getUser()
	{
		String user = this.propertiesXML.getProperty("config.share.ftp.user");
		if(Util.isNull(user)){
			user="";
		}
    
		return user;
	}
  
	/**
	 * ����ƽ̨�ϴ�ftp ��������
	 * @return
	 */
	public String getPwd()
	{
		String pwd = this.propertiesXML.getProperty("config.share.ftp.pwd");
		if(Util.isNull(pwd)){
			pwd="";
		}
		return pwd;
	}
	
	/**
	 * ����ƽ̨�ϴ�ftp ������������
	 * @return
	 */
	public String getPassiveMode()
	{
		String passiveMode = this.propertiesXML.getProperty("config.share.ftp.passiveMode");
		if(Util.isNull(passiveMode)){
			passiveMode="";
		}
		return passiveMode;
	}
  
	/**
	 * ����ƽ̨·��ԭʼ���ݴ��Ŀ¼
	 * @return
	 */
	public String getRemoteRootDT()
	{
		String remoteRoot = this.propertiesXML.getProperty("config.share.ftp.remoteRootDT");
		if(Util.isNull(remoteRoot)){
			remoteRoot="";
		}
    
		return remoteRoot;
	}
  
	/**
	 * ����ƽ̨·���������ݴ��Ŀ¼
	 * @return
	 */
	public String getRemoteRootPM()
	{
		String remoteRoot = this.propertiesXML.getProperty("config.share.ftp.remoteRootPM");
		if(Util.isNull(remoteRoot)){
			remoteRoot="";
		}
		return remoteRoot;
	}
  
	/**
	 * FTP�����ʽ
	 * @return
	 */
	public String getEncoding()
	{
		String encoding = this.propertiesXML.getProperty("config.share.ftp.encoding");
		if(Util.isNull(encoding)){
			encoding="";
		}
		return encoding;
	}
  
	/**
	 * ��ȡ�Ƿ����gp�ϴ��������ļ�
	 * @return
	 */
	public boolean isGp(){
	  	boolean b = false;
	    String str = this.propertiesXML.getProperty("config.gp.enable");
	    if (Util.isNull(str)) return b;
	    str = str.toLowerCase().trim();
	    if ((str.equals("on")) || (str.equals("true")))
	    {
	      b = true;
	    }
	    else if ((str.equals("off")) || (str.equals("false")))
	    {
	      b = false;
	    }
	    return b;
	}
	
	/**
	 * GP��������ϴ�FTP��IP
	 * @return
	 */
	public String getGpIp()
	{
		String ip = this.propertiesXML.getProperty("config.gp.ftp.ip");
		if(Util.isNull(ip)){
			ip="";
		}
    
		return ip;
	}
  
	/**
	 * GP��������ϴ�FTP��PORT
	 * @return
	 */
	public String getGpPort()
	{
		String port = this.propertiesXML.getProperty("config.gp.ftp.port");
		if(Util.isNull(port)){
			port="";
		}
		return port;
	}
  
	/**
	 * GP FTP �û�
	 * @return
	 */
	public String getGpUser()
	{
		String user = this.propertiesXML.getProperty("config.gp.ftp.user");
		if(Util.isNull(user)){
			user="";
		}
		return user;
	}
  
	/**
	 * GP FTP  ����
	 * @return
	 */
	public String getGpPwd()
	{
		String pwd = this.propertiesXML.getProperty("config.gp.ftp.pwd");
		if(Util.isNull(pwd)){
			pwd="";
		}
    
		return pwd;
	}
	
	/**
	 * GP FTP ����ģʽ
	 * @return
	 */
	public String getGpPassiveMode()
	{
		String passiveMode = this.propertiesXML.getProperty("config.gp.ftp.passiveMode");
		if(Util.isNull(passiveMode)){
			passiveMode="";
		}
		return passiveMode;
	}
  
	/**
	 * GP FTP �ϴ�·��ԭʼ����Ŀ¼
	 * @return
	 */
	public String getGpRemoteRootDT()
	{
		String remoteRoot = this.propertiesXML.getProperty("config.gp.ftp.remoteRootDT");
		if(Util.isNull(remoteRoot)){
			remoteRoot="";
		}
		return remoteRoot;
	}
  
	/**
	 * GP FTP �ϴ�·����������Ŀ¼
	 * @return
	 */
	public String getGpRemoteRootPM()
	{
		String remoteRoot = this.propertiesXML.getProperty("config.gp.ftp.remoteRootPM");
		if(Util.isNull(remoteRoot)){
			remoteRoot="";
		}
		return remoteRoot;
	}
  
	/**
	 * GP FTP �����ʽ
	 * @return
	 */
	public String getGpEncoding()
  	{
		String encoding = this.propertiesXML.getProperty("config.gp.ftp.encoding");
		if(Util.isNull(encoding)){
			encoding="";
		}
		return encoding;
	}
	  
	/**
	 * ���ݿ����ӳ�����
	 * @return
	 */
	public String getPoolType()
	{
		String name = this.propertiesXML.getProperty("config.db.type");
		if (Util.isNull(name))
		{
			name = "javax.sql.DataSource";
		}
		return name;
	}

	/**
	 * ���ݿ�����
	 * @return
	 */
	public String getDbDriver()
	{
		String d = this.propertiesXML.getProperty("config.db.driverClassName");

		if (Util.isNull(d))
		{
			d = "oracle.jdbc.driver.OracleDriver";
		}
		return d;
	}
	
	/**
	 * ���ݿ�����
	 * @return
	 */
	public String getGPDbDriver()
	{
		String d = this.propertiesXML.getProperty("config.gpdb.driverClassName");

		if (Util.isNull(d))
		{
			d = "oracle.jdbc.driver.OracleDriver";
		}
		return d;
	}
	
	/**
	 * ���ݿ�����
	 * @return
	 */
	public String getHiveDbDriver()
	{
		String d = this.propertiesXML.getProperty("config.hivedb.driverClassName");

		if (Util.isNull(d))
		{
			d = "org.apache.hadoop.hive.jdbc.HiveDriver";
		}
		return d;
	}

	/**
	 * ���ݿ������ַ���
	 * @return
	 */
	public String getDbUrl()
	{
		String url = this.propertiesXML.getProperty("config.db.url");

		if (Util.isNull(url))
		{
			url = "";
		}
		return url;
	}
	
	/**
	 * ���ݿ������ַ���
	 * @return
	 */
	public String getGPDbUrl()
	{
		String url = this.propertiesXML.getProperty("config.gpdb.url");

		if (Util.isNull(url))
		{
			url = "";
		}
		return url;
	}
	
	/**
	 * ���ݿ������ַ���
	 * @return
	 */
	public String getHiveDbUrl()
	{
		String url = this.propertiesXML.getProperty("config.hivedb.url");

		if (Util.isNull(url))
		{
			url = "";
		}
		return url;
	}

	/**
	 * ���ݿ������
	 * @return
	 */
	public String getDbService()
	{
		String service = this.propertiesXML.getProperty("config.db.service");
		if (Util.isNull(service))
		{
			service = "";
		}
		return service;
	}

	/**
	 * ���ݿ��¼�û�������
	 * @return
	 */
	public String getDbUserName()
	{
		if (this.realDbUser != null) return this.realDbUser;
		String user = this.propertiesXML.getProperty("config.db.user");
		if (Util.isNull(user))
		{
			user = "login";
		}
		return user;
	}
	
	/**
	 * ���ݿ��¼�û�������
	 * @return
	 */
	public String getGPDbUserName()
	{
		if (this.realDbUser != null) return this.realDbUser;
		String user = this.propertiesXML.getProperty("config.gpdb.user");
		if (Util.isNull(user))
		{
			user = "login";
		}
		return user;
	}
	
	/**
	 * ���ݿ��¼�û�������
	 * @return
	 */
	public String getHiveDbUserName()
	{
		if (this.realDbUser != null) return this.realDbUser;
		String user = this.propertiesXML.getProperty("config.hivedb.user");
		if (Util.isNull(user))
		{
			user = "login";
		}
		return user;
	}


	/**
	 * ���ݿ��¼�û�������
	 * @return
	 */
	public String getDbPassword()
	{
		if (this.realDbPwd != null) return this.realDbPwd;
		String pwd = this.propertiesXML.getProperty("config.db.password");
		if (Util.isNull(pwd))
		{
			pwd = "login";
		}
		return pwd;
	}
	
	/**
	 * ���ݿ��¼�û�������
	 * @return
	 */
	public String getGPDbPassword()
	{
		if (this.realDbPwd != null) return this.realDbPwd;
		String pwd = this.propertiesXML.getProperty("config.gpdb.password");
		if (Util.isNull(pwd))
		{
			pwd = "login";
		}
		return pwd;
	}

	/**
	 * ����߳�
	 * @return
	 */
	public int getPoolMaxActive()
	{
		int ma = 12;
		try
		{
			ma = Integer.parseInt(this.propertiesXML.getProperty("config.db.maxActive"));
		}
		catch (Exception localException)
		{
		}
		if (ma <= 0)
		{
			ma = 12;
		}
		return ma;
	}

	/**
	 * ����߳���
	 * @return
	 */
	public int getPoolMaxIdle()
	{
		int maxIdle = 5;
		try
		{
			maxIdle = Integer.parseInt(this.propertiesXML.getProperty("config.db.maxIdle"));
		}
		catch (Exception localException)
		{
		}
		if (maxIdle <= 0)
		{
			maxIdle = 5;
		}
		return maxIdle;
	}

	/**
	 * �̵߳ȴ�
	 * @return
	 */
	public int getPoolMaxWait()
	{
		int maxWait = 10000;
		try
		{
			maxWait = Integer.parseInt(this.propertiesXML.getProperty("config.db.maxWait"));
		}
		catch (Exception localException)
		{
		}
		if (maxWait <= 0)
		{
			maxWait = 10000;
		}
		return maxWait;
	}

	/**
	 * DB ��ѯ��ʱʱ��
	 * @return
	 */
	public int getQueryTimeout()
	{
		int timeout = 180;
		try
		{
			timeout = Integer.parseInt(this.propertiesXML.getProperty("config.db.queryTimeout"));
		}
		catch (Exception localException)
		{
		}
		if (timeout <= 0)
		{
			timeout = 180;
		}
		return timeout;
	}

	/**
	 * ���ݿ��һ��������֤SQL
	 * @return
	 */
	public String getDbValidationQueryString()
	{
		String sql = this.propertiesXML.getProperty("config.db.validationQuery");
		if (Util.isNull(sql))
		{
			sql = "select sysdate from dual";
		}
		return sql;
	}

	/**
	 * 
	 * @return
	 */
	public String getProjectName()
	{
		String projectName = this.propertiesXML.getProperty("config.system.projectName");
		if (Util.isNull(projectName))
		{
			projectName = "CapricornV2";
		}
		return projectName;
	}

	/**
	 * ��ǰ��������Ŀ¼
	 * @return
	 */
	public String getCurrentPath()
	{
		String path = this.propertiesXML.getProperty("config.system.currentPath");
		if (Util.isNull(path))
		{
			path = "";
		}
		return path;
	}

	public String getFdPath()
	{
		String path = this.propertiesXML.getProperty("config.system.fdPath");
		if (Util.isNull(path))
		{
			path = "";
		}
		return path;
	}

	public String getMROutputPath()
	{
		String str = this.propertiesXML.getProperty("config.mr.mrOutputPath");
		if (Util.isNull(str))
		{
			str = "";
		}
		return str;
	}

	/**
	 * �ɼ�ģ��·��
	 * @return
	 */
	public String getTempletPath()
	{
		String str = this.propertiesXML.getProperty("config.system.templetFilePath");
		if (Util.isNull(str))
		{
			str = "";
		}
		return str;
	}
	
	/**
	 * �ɼ�ģ��·��
	 * @return
	 */
	public String getTaskConfigPath()
	{
		String str = this.propertiesXML.getProperty("config.system.taskFilePath");
		if (Util.isNull(str))
		{
			str = "";
		}
		return str;
	}

	/**
	 * �ɼ��˿�
	 * @return
	 */
	public int getCollectPort()
	{
		int port = 0;
		try
		{
			port = Integer.parseInt(this.propertiesXML.getProperty("config.system.port"));
		}
		catch (Exception localException)
		{
		}
		if (port <= 0)
		{
			port = 0;
    	}
		return port;
	}

	/**
	 * �Ƿ�ɾ����־
	 * @return
	 */
	public boolean isDeleteLog()
	{
		boolean b = true;
		try
		{
			b = Boolean.parseBoolean(this.propertiesXML.getProperty("config.externalTool.sqlldr.isDelLog"));
		}
		catch (Exception localException)
		{
		}
		return b;
	}

	public int getMRSource()
	{
		int i = 1;
		try
		{
			i = Integer.parseInt(this.propertiesXML.getProperty("config.mr.mrSource"));
		}
		catch (Exception localException)
		{
		}
		if (i <= 0)
		{
			i = 1;
		}
		return i;
	}

	/**
	 * SQL load �������ʽ
	 * @return
	 */
	public String getSqlldrCharset()
	{
		String s = this.propertiesXML.getProperty("config.externalTool.sqlldr.charset");
		
		if (Util.isNull(s))
		{
			s = "ZHS16GBK";
		}

		return s;
	}
  
  	public String getreadsize()
  	{
  		String s = this.propertiesXML.getProperty("config.externalTool.sqlldr.readsize");

  		if (Util.isNull(s))
  		{
  			s = "readsize=1048576";
  		}
  		else
  		{
  			s = "readsize="+s;
  		}

  		return s;
  	}

  	public int getFrontNum()
  	{
  		int i = 1;
  		try
  		{
  			i = Integer.parseInt(this.propertiesXML.getProperty("config.mr.frontNum"));
  		}
  		catch (Exception localException)
  		{
  		}
  		if (i <= 0)
  		{
  			i = 1;
  		}
  		return i;
  	}

  	public boolean isMRSingleCal()
  	{
  		boolean b = true;
  		try
  		{
  			b = Boolean.parseBoolean(this.propertiesXML.getProperty("config.mr.mrSingleCal"));
  		}
  		catch (Exception localException)
  		{
  		}
  		return b;
  	}

  	public String getWinrarPath()
  	{
  		String str = this.propertiesXML.getProperty("config.system.zipTool");
  		if (Util.isNull(str))
  		{
  			str = "";
  		}
  		return str;
  	}

  	public String getTraceFileter2Path()
  	{
  		String str = this.propertiesXML.getProperty("config.externalTool.traceFileter2Path");
  		if (Util.isNull(str))
  		{
  			str = "";
  		}
  		return str;
  	}

  	/**
  	 * ����߳���
  	 * @return
  	 */
  	public int getMaxThread()
  	{
  		int i = 15;
  		try
  		{
  			i = Integer.parseInt(this.propertiesXML.getProperty("config.system.maxThreadCount"));
  		}
  		catch (Exception localException)
  		{
  		}
  		if (i < 0)
  		{
  			i = 0;
  		}
  		return i;
  	}

  	/**
  	 * ��󲹲��߳�
  	 * @return
  	 */
  	public int getMaxCountPerRegather()
  	{
  		int i = 10;
  		try
  		{
  			i = Integer.parseInt(this.propertiesXML.getProperty("config.system.maxCountPerRegather"));
  		}
  		catch (Exception localException)
  		{
  		}
  		if (i <= 0)
  		{
  			i = 10;
  		}
  		return i;
  	}

  	
  	public float getSiteDistRange()
  	{
  		float f = 0.0F;
  		try
  		{
  			f = Float.parseFloat(this.propertiesXML.getProperty("config.mr.siteDistRange"));
  		}
  		catch (Exception localException)
  		{
  		}
  		return f;
 	}

  	public String getLifecycleFileExt()
  	{
  		String str = this.propertiesXML.getProperty("config.module.dataFileLifecycle.fileExt");
  		if (Util.isNull(str))
  		{
  			str = ".flag";
  		}
  		return str;
  	}

  	/**
  	 * �ɼ��ļ�����������
  	 * @return
  	 */
  	public int getFilecycle()
  	{
  		int i = 20;
  		try
  		{
  			i = Integer.parseInt(this.propertiesXML.getProperty("config.module.dataFileLifecycle.lifecycle"));
  		}
  		catch (Exception localException)
  		{
  		}
  		if (i < 0)
  		{
  			i = 20;
  		}
  		return i;
  	}

  	/**
  	 * ���������ڿ�����Чʱ���Ƿ�ֱ��ɾ����ʹ�õ��ļ�
  	 * @return
  	 */
  	public boolean isDeleteWhenOff()
  	{
  		boolean b = true;
  		try
  		{
  			b = Boolean.parseBoolean(this.propertiesXML.getProperty("config.module.dataFileLifecycle.delWhenOff"));
  		}
  		catch (Exception localException)
  		{
  		}
  		return b;
  	}


  	/**
  	 * ϵͳ��ǰ�汾��
  	 * @return
  	 */
  	public String getEdition()
  	{
  		String e = this.propertiesXML.getProperty("config.system.version.edition");
  		if (Util.isNull(e))
  		{
  			e = "";
  		}
  		return e;
  	}

  	/**
  	 * �汾����ʱ��
  	 * @return
  	 */
  	public String getReleaseTime()
  	{
  		String d = this.propertiesXML.getProperty("config.system.version.releaseTime");
  		if (Util.isNull(d)) return "";
  		try
  		{
  			d = Util.getDateString(Util.getDate1(d));
  		}
  		catch (Exception e)
  		{
  			return "";
  		}
  		return d;
  	}

  	/**
  	 * ϵͳ�澯����
  	 * @return
  	 */
  	public boolean isEnableAlarm()
  	{
  		boolean b = false;
  		String on = this.propertiesXML.getProperty("config.module.alarm.enable");
  		if (Util.isNotNull(on))
  		{
  			on = on.toLowerCase().trim();
  			if ((on.equals("on")) || (on.equals("true")))
  				b = true;
  		}
  		return b;
  	}

  	/**
  	 * �澯����
  	 * @return
  	 */
  	public List<String> getFilters()
  	{
  		return this.propertiesXML.getPropertyes("config.module.alarm.filters.newAlarm.filter");
  	}

  	public String getSender()
  	{
  		String sender = null;
  		sender = this.propertiesXML.getProperty("config.module.alarm.senderBean");
  		return sender;
  	}

  	public String getMailSMTPHost()
  	{
  		String host = null;
  		host = this.propertiesXML.getProperty("config.externalTool.mail.smtp_host");
  		return host;
  	}

  	public String getMailAccount()
  	{
  		String account = null;
  		account = this.propertiesXML.getProperty("config.externalTool.mail.user");
  		return account;
  	}

  	public String getMailPassword()
  	{
  		String pwd = null;
  		pwd = this.propertiesXML.getProperty("config.externalTool.mail.password");
  		return pwd;
  	}

  	public String[] getMailTO()
  	{
  		String[] tos = (String[])null;
  		String to = this.propertiesXML.getProperty("config.externalTool.mail.to");
  		if (Util.isNotNull(to))
  		{
  			tos = to.split(";");
  		}
  		return tos;
  	}

  	public String[] getMailCC()
  	{
  		String[] ccs = (String[])null;
  		String cc = this.propertiesXML.getProperty("config.externalTool.mail.cc");
  		if (Util.isNotNull(cc))
  		{
  			ccs = cc.split(";");
  		}
  		return ccs;
  	}

  	public String[] getMailBCC()
  	{
  		String[] bccs = (String[])null;
  		String bcc = this.propertiesXML.getProperty("config.externalTool.mail.bcc");
  		if (Util.isNotNull(bcc))
  		{
  			bccs = bcc.split(";");
  		}
  		return bccs;
  	}

  	/**
  	 * �ļ��������ڿ��ƿ���
  	 * @return
  	 */
  	public boolean isEnableDataFileLifecycle()
  	{
  		boolean b = false;
  		String str = this.propertiesXML.getProperty("config.module.dataFileLifecycle.enable");
    	if (Util.isNull(str)) return b;
    	str = str.toLowerCase().trim();
    	if ((str.equals("on")) || (str.equals("true")))
    	{
    		b = true;
    	}
    	else if ((str.equals("off")) || (str.equals("false")))
    	{
    		b = false;
    	}
    	return b;
  	}

  	public float getFieldMatch()
  	{
  		String str = this.propertiesXML.getProperty("config.system.fieldMatch");
  		float f = 0.8F;
  		try
  		{
  			f = Float.parseFloat(str);
  		}
  		catch (Exception e)
  		{
  			return 0.8F;
  		}
  		return f;
  	}

  	/**
  	 * ���ɼ�����
  	 * @return
  	 */
  	public int getMaxCltCount()
  	{
  		String str = this.propertiesXML.getProperty("config.system.maxCltCount");
  		int i = 200;
  		try
  		{
  			i = Integer.parseInt(str);
  		}
  		catch (Exception localException)
  		{
  		}
  		return i;
  	}

  	/**
  	 * ��󲹲�����
  	 * @return
  	 */
  	public int getMaxRecltCount()
  	{
  		String str = this.propertiesXML.getProperty("config.system.maxRecltCount");
  		int i = 10;
  		try
  		{
  			i = Integer.parseInt(str);
  		}
  		catch (Exception localException)
  		{
  		}
  		return i;
  	}

  	public boolean isEnableWeb()
  	{
  		boolean b = false;
  		String str = this.propertiesXML.getProperty("config.module.web.enable");
  		if (Util.isNull(str)) return b;
  		str = str.toLowerCase().trim();
  		if ((str.equals("on")) || (str.equals("true")))
    	{
  			b = true;
    	}
  		else if ((str.equals("off")) || (str.equals("false")))
  		{
  			b = false;
  		}
  		return b;
  	}

  	public int getWebPort()
  	{
  		int port = 8080;
  		try
  		{
  			port = Integer.parseInt(this.propertiesXML.getProperty("config.module.web.port"));
  		}
  		catch (Exception localException)
  		{
  		}
  		if (port <= 0)
  		{
  			port = 8080;
  		}
  		return port;
  	}

  	public String getWebServerClass()
  	{
  		return this.propertiesXML.getProperty("config.module.web.httpServer.class");
  	}

  	public String getWebApp()
  	{
  		return this.propertiesXML.getProperty("config.module.web.httpServer.webapp");
  	}

  	public String getWebContextPath()
  	{
  		return this.propertiesXML.getProperty("config.module.web.httpServer.contextpath");
  	}

  	public String getWebCharset()
  	{
  		return this.propertiesXML.getProperty("config.module.web.charset");
  	}

  	public String getWebServerLogLevel()
  	{
  		String str = this.propertiesXML.getProperty("system.web.httpServer.loglevel");
  		if ((str == null) || (str.equals("")) || (str.equalsIgnoreCase("info")))
  			str = "1";
  		else if (str.equalsIgnoreCase("debug"))
  			str = "0";
  		else if (str.equalsIgnoreCase("warn"))
  			str = "2";
  		else if (str.equalsIgnoreCase("error"))
  			str = "3";
  		else if (str.equalsIgnoreCase("fatal"))
  			str = "4";
  		else {
  			str = "1";
  		}
  		return str;
  	}

  	public int getDataLogInterval()
  	{
  		int interval = 100;
  		try
  		{
  			interval = Integer.parseInt(this.propertiesXML.getProperty("config.module.dataLog.interval"));
  		}
  		catch (Exception localException)
  		{
  		}
  		if (interval <= 0)
  		{
  			interval = 100;
  		}
  		return interval;
  	}

  	public boolean isEnableDataLog()
  	{
  		String str = this.propertiesXML.getProperty("config.module.dataLog.enable");
  		if (Util.isNull(str)) return false;
  		return str.trim().equalsIgnoreCase("on");
  	}

  	public boolean isSqlldrDataLog()
  	{
  		String str = this.propertiesXML.getProperty("config.module.dataLog.sqlldrMode");
  		if (Util.isNull(str)) return false;
  		return str.trim().equalsIgnoreCase("true");
  	}

  	public boolean isDelDataLogTmpFile()
  	{
  		String str = this.propertiesXML.getProperty("config.module.dataLog.delTmpFile");
  		if (Util.isNull(str)) return true;
  		return str.trim().equalsIgnoreCase("true");
  	}

  	public boolean isEnableDelayProbe()
  	{
  		String str = this.propertiesXML.getProperty("config.module.delayProbe.enable");
  		if (Util.isNull(str)) return false;
  		return str.trim().equalsIgnoreCase("on");
  	}

  	public int getDelayProbeTimes()
  	{
  		String str = this.propertiesXML.getProperty("config.module.delayProbe.probeTimes");
  		try
  		{
  			int times = Integer.parseInt(str);
  			return times;
  		}
  		catch (Exception localException)
  		{
    	}

  		return 5;
  	}

  	public int getProbeInterval()
  	{
  		String str = this.propertiesXML.getProperty("config.module.delayProbe.interval");
  		try
  		{
  			int interval = Integer.parseInt(str);
  			if (interval <= 0) return 5;
  			return interval;
  		}
  		catch (Exception localException)
  		{
  		}

  		return 5;
  	}

  	public boolean isProbeFTP()
  	{
  		String str = this.propertiesXML.getProperty("config.module.delayProbe.ftp");
  		try
  		{
  			return str.trim().equalsIgnoreCase("true");
  		}
  		catch (Exception localException)
  		{
  		}
  		return false;
  	}
  	
  	public List<String> getReservALRnc()
  	{
  		List list = new ArrayList();
  		String str = this.propertiesXML.getProperty("config.w-al-reserv-rnc");
  		if (Util.isNotNull(str))
  		{
  			String[] ss = str.split(",");
  			for (String s : ss)
  			{
  				if (Util.isNotNull(s))
  					list.add(s.trim());
  			}
  		}
  		return list;
  	}

  	public boolean isEnableProbeLog()
  	{
  		String str = this.propertiesXML.getProperty("config.module.delayProbe.log");
  		try
  		{
  			return str.trim().equalsIgnoreCase("true");
  		}
  		catch (Exception localException)
  		{
  		}
  		return true;
  	}
  
   /**
	 * �Ƿ�����·�����
	 * @return
	 */
	public boolean isEnableRunDTStatistic()
	{
		boolean b = true;
		String str = this.propertiesXML.getProperty("config.dt.enable");
		if (Util.isNull(str)) return b;
		str = str.toLowerCase().trim();
		if ((str.equals("on")) || (str.equals("true")))
		{
			b = true;
		}
		else if ((str.equals("off")) || (str.equals("false")))
		{
			b = false;
		}
		return b;
	}
	
	/**
	 * �Ƿ�ɾ��·�������Ҫ��ԭʼ�ļ�
	 * @return
	 */
	public boolean IsDeleteDTFile()
	{
		String str = this.propertiesXML.getProperty("config.dt.isDelFile");
		if (Util.isNull(str)) return false;
		if(str.toUpperCase().equals("TRUE"))
		{
			return true;
		}
		else if(str.toUpperCase().equals("FALSE"))
		{
			return false;
		}
		return false;
	}
	
	/**
	 * �ɼ������Ƿ�ʹ��XML�ļ�����
	 * @return
	 */
	public boolean IsTaskUserXML()
	{
		String str = this.propertiesXML.getProperty("config.task.usexml");
		if (Util.isNull(str)) return false;
		if(str.toUpperCase().equals("TRUE") || str.toUpperCase().equals("YES"))
		{
			return true;
		}
		else if(str.toUpperCase().equals("FALSE") || str.toUpperCase().equals("NO"))
		{
			return false;
		}
		return false;
	}
	
	/**
	 * �õ�����ͼ���·��
	 * @return
	 */
	public String GetMapPath()
	{
		String str = this.propertiesXML.getProperty("config.map.PATH");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	public String GetDTDBServer()
	{
		String str = this.propertiesXML.getProperty("config.dt.DBServer");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	public String GetDTDBUserID()
	{
		String str = this.propertiesXML.getProperty("config.dt.userid");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	public String GetDTDBPassword()
	{
		String str = this.propertiesXML.getProperty("config.dt.password");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * WebService ��ַ
	 * @return
	 */
	public String UteleServiceUrl()
	{
		String str = this.propertiesXML.getProperty("config.system.UteleServiceUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * ���Žӿ� ��ַ
	 * @return
	 */
	public String UteleSMSUrl()
	{
		String str = this.propertiesXML.getProperty("config.system.UteleSMSUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	
	/**
	 * Http ����ַ
	 * @return
	 */
	public String UteleLoaderUrl()
	{
		String str = this.propertiesXML.getProperty("config.share.UteleLoaderUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http ��Ȩ��ַ
	 * @return
	 */
	public String UteleCheckUrl()
	{
		String str = this.propertiesXML.getProperty("config.share.UteleCheckUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http �ͷŵ�ַ
	 * @return
	 */
	public String UteleLogoutUrl()
	{
		String str = this.propertiesXML.getProperty("config.share.UteleLogoutUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http ��Ȩ�û�
	 * @return
	 */
	public String HttpCheckUser()
	{
		String str = this.propertiesXML.getProperty("config.share.check.username");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http ��Ȩ�û�����
	 * @return
	 */
	public String HttpCheckPassword()
	{
		String str = this.propertiesXML.getProperty("config.share.check.password");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http ��Ȩ�û�����
	 * @return
	 */
	public String TaurusSMS()
	{
		String str = this.propertiesXML.getProperty("config.specialapp.taurussms");
		if (Util.isNull(str)) return "13922890531";
		return str;
	}

	public static void main(String[] args)
	{
		System.out.println(getInstance().getDbUserName());
		System.out.println(getInstance().getDbPassword());
	}
}