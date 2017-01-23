package com.turk.clusters.slave;

import java.io.File;

import org.apache.log4j.Logger;

import com.turk.config.SystemConfigException;
import com.turk.util.LogMgr;
import com.turk.util.PropertiesXML;
import com.turk.util.Util;

/**
 * Slave配置
 * @author Administrator
 *
 */
public class SlaveConfig {
private PropertiesXML propertiesXML;
	
	/**
	 * 配置文件
	 */
	private static final String SLAVEFILE = "." + File.separator + "config" + 
    	File.separator + "slaveconfig.xml";
	
	private static final Logger logger = LogMgr.getInstance().getAppLogger("slave");

	private static SlaveConfig _instance = null;

	private SlaveConfig()
    	throws SystemConfigException
    {
		this.propertiesXML = new PropertiesXML(SLAVEFILE);
    }

	public static SlaveConfig getInstance()
	{
		if (_instance == null)
		{
			try
			{
				_instance = new SlaveConfig();
			}
			catch (SystemConfigException e)
			{
				logger.error("创建SystemConfig对象时出现异常", e);
				return null;
			}
		}
		return _instance;
	}

	
	public String getMasterServer()
	{
		String dbName = this.propertiesXML.getProperty("slaveconfig.master.server");

		if (Util.isNull(dbName))
		{
			dbName = "localhost";
		}
		return dbName;
	}
	
	public int getMasterPort()
	{
		String port = this.propertiesXML.getProperty("slaveconfig.master.port");

		if (Util.isNull(port))
		{
			port = "9527";
		}
		return Integer.parseInt(port);
	}
	
	public String getSlaveServer()
	{
		String dbName = this.propertiesXML.getProperty("slaveconfig.slave.server");

		if (Util.isNull(dbName))
		{
			dbName = "localhost";
		}
		return dbName;
	}
	
	public int getSlavePort()
	{
		String port = this.propertiesXML.getProperty("slaveconfig.slave.port");

		if (Util.isNull(port))
		{
			port = "9528";
		}
		return Integer.parseInt(port);
	}
	
	public int getSlaveFlag()
	{
		String flag = this.propertiesXML.getProperty("slaveconfig.slave.flag");

		if (Util.isNull(flag))
		{
			flag = "1";
		}
		return Integer.parseInt(flag);
	}

}
