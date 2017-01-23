package com.turk.clusters.master;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.turk.util.Util;
import com.turk.util.LogMgr;
import com.turk.bean.PBeanMgr;
import com.turk.console.ConsoleMgr;
import com.turk.framework.SelfTest;
import com.turk.framework.Version;

/**
 * Master 主节点
 * @author Administrator
 *
 */
public class MasterMain {
	
	private static Logger log = LogMgr.getInstance().getAppLogger("master");
	
	public static void main(String[] args)
	{
		
		log.info("Master 启动.");
		Version ver = Version.getInstance();
		String strVer = ver.getExpectedVersion();
		if (!ver.isRightVersion())
		{
			log.error("系统退出. 原因:版本号不一致. 内部版本号:" + strVer);
			return;
		}
		log.info("版本号:" + strVer);
		try
		{
			//管理控制台
			int port = 9020;
			ConsoleMgr.getInstance(port).start();
		}
		catch (Exception e)
		{
			log.error("采集系统启动失败,原因: 控制台模块启动异常. ", e);
		}
		
		//自检模块
		SelfTest st = SelfTest.getInstance();
		if(!st.IsOK())
		{
			log.error("系统退出. 原因:程序自检异常");
			return;
		}

		//打印环境变量
		Util.printEnvironmentInfo();

		//加载bean
		PBeanMgr.getInstance();

		//AlarmMgr.getInstance();
		
		//ResourceManagerment.getInstance().start();

		//DataLifecycleMgr.getInstance().start();

		//主节点
		MasterMain.getInstance();
		
		//采集任务模块
		//ScanThread scanThread = ScanThread.getInstance();
		//scanThread.startScan();	
	}
	
	
	private static MasterMain _instance;
	
	public static synchronized MasterMain getInstance()
	{
		if (_instance == null)
		{
			_instance = new MasterMain();
		}
		return _instance;
	}
	
	public MasterMain()
	{
		MasterConsole console = new MasterConsole();
		try {
			console.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("Master 启动异常",e);
		}
	}
}
