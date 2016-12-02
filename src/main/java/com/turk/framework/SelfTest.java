package com.turk.framework;

import java.io.File;

import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.util.LogMgr;

/**
 * 自检模块
 * @author Administrator
 *
 */
public class SelfTest {

	private static SelfTest _instance = null;
	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	
	public static synchronized SelfTest getInstance()
	{
		if (_instance == null)
		{
			_instance = new SelfTest();
		}

		return _instance;
	}
	
	public boolean IsOK()
	{
		boolean IsOk = false;
		//检查采集目录Data配置是否正确
		File fData = new File(SystemConfig.getInstance().getCurrentPath());
		if(fData.exists())
		{
			log.debug("检查采集数据存放目录是否存在...OK!");
			IsOk = true;
		}
		else
		{
			log.debug("检查采集数据存放目录是否存在...Failure!");
			IsOk = false;
		}
		
		File fTemplate = new File(SystemConfig.getInstance().getTempletPath());
		if(fTemplate.exists())
		{
			log.debug("检查采集数据模版目录是否存在...OK!");
			IsOk = true;
		}
		else
		{
			log.debug("检查采集数据模版目录是否存在...Failure!");
			IsOk = false;
		}
		
		return IsOk;
	}
}
