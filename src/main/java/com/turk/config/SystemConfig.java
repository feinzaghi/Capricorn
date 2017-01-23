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
 * 版权所有。 
 *
 * 文件名：SystemConfig.java
 * 文件功能描述：系统配置
 * 
 * 创建日期：
 *
 * 修改日期：
 * 修改描述：
 *
 * 修改日期：
 * 修改描述：
 */
public class SystemConfig
{
	private PropertiesXML propertiesXML;
	
	/**
	 * 配置文件
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
				logger.error("创建SystemConfig对象时出现异常", e);
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
	 * 获取是否进行共享采集的配置文件
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
	 * 读取创新平台数据是否上传ftp配置
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
	 * 创新平台开始采集时间
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
	 * 创新平台上传ftp IP配置
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
	 * 创新平台上传ftp 端口配置
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
	 * 创新平台上传ftp 用户配置
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
	 * 创新平台上传ftp 密码配置
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
	 * 创新平台上传ftp 加密类型配置
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
	 * 创新平台路测原始数据存放目录
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
	 * 创新平台路测性能数据存放目录
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
	 * FTP编码格式
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
	 * 获取是否进行gp上传的配置文件
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
	 * GP入库数据上传FTP的IP
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
	 * GP入库数据上传FTP的PORT
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
	 * GP FTP 用户
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
	 * GP FTP  密码
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
	 * GP FTP 密码模式
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
	 * GP FTP 上传路测原始数据目录
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
	 * GP FTP 上传路测性能数据目录
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
	 * GP FTP 编码格式
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
	 * 数据库连接池类型
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
	 * 数据库驱动
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
	 * 数据库驱动
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
	 * 数据库驱动
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
	 * 数据库连接字符串
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
	 * 数据库连接字符串
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
	 * 数据库连接字符串
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
	 * 数据库服务名
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
	 * 数据库登录用户名密码
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
	 * 数据库登录用户名密码
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
	 * 数据库登录用户名密码
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
	 * 数据库登录用户名密码
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
	 * 数据库登录用户名密码
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
	 * 最大活动线程
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
	 * 最大线程数
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
	 * 线程等待
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
	 * DB 查询超时时长
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
	 * 数据库第一次连接验证SQL
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
	 * 当前程序运行目录
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
	 * 采集模版路径
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
	 * 采集模版路径
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
	 * 采集端口
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
	 * 是否删除日志
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
	 * SQL load 入库编码格式
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
  	 * 最大线程数
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
  	 * 最大补采线程
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
  	 * 采集文件的生命周期
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
  	 * 当生命周期控制无效时，是否直接删除已使用的文件
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
  	 * 系统当前版本号
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
  	 * 版本升级时间
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
  	 * 系统告警开关
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
  	 * 告警过滤
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
  	 * 文件生命周期控制开关
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
  	 * 最大采集任务
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
  	 * 最大补采任务
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
	 * 是否启动路测汇总
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
	 * 是否删除路测汇总需要的原始文件
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
	 * 采集任务是否使用XML文件配置
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
	 * 得到加载图层的路径
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
	 * WebService 地址
	 * @return
	 */
	public String UteleServiceUrl()
	{
		String str = this.propertiesXML.getProperty("config.system.UteleServiceUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * 短信接口 地址
	 * @return
	 */
	public String UteleSMSUrl()
	{
		String str = this.propertiesXML.getProperty("config.system.UteleSMSUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	
	/**
	 * Http 入库地址
	 * @return
	 */
	public String UteleLoaderUrl()
	{
		String str = this.propertiesXML.getProperty("config.share.UteleLoaderUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http 鉴权地址
	 * @return
	 */
	public String UteleCheckUrl()
	{
		String str = this.propertiesXML.getProperty("config.share.UteleCheckUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http 释放地址
	 * @return
	 */
	public String UteleLogoutUrl()
	{
		String str = this.propertiesXML.getProperty("config.share.UteleLogoutUrl");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http 鉴权用户
	 * @return
	 */
	public String HttpCheckUser()
	{
		String str = this.propertiesXML.getProperty("config.share.check.username");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http 鉴权用户密码
	 * @return
	 */
	public String HttpCheckPassword()
	{
		String str = this.propertiesXML.getProperty("config.share.check.password");
		if (Util.isNull(str)) return "";
		return str;
	}
	
	/**
	 * Http 鉴权用户密码
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