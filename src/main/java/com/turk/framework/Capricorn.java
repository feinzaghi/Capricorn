package com.turk.framework;

//import alarm.AlarmMgr;
//import alarm.ProcessStatus;

import java.util.Date;

import org.apache.log4j.Logger;



//import specialapp.taurus.utele.FlexAuthServer;
//import specialapp.taurus.utele.MonitorServer;
import com.turk.Config.SystemConfig;
import com.turk.app.appinterface;
import com.turk.console.ConsoleMgr;
import com.turk.util.LogMgr;
import com.turk.util.Util;

/**
 * Capricorn V2 平台主入口
 * @author 
 * Capricorn(摩羯座)
 *   有过人的耐力、意志坚定、有时间观念、有责任感、重视权威和名声，
 *   对领导统御很有一套，自成一格，组织能力也很不错。
 *   
 * V2版本说明：
 *    V2是源于V1版本的性能提升版本，增加了采集管理功能，提升了采集维护的效率
 */
public class Capricorn
{
	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public static final Date SYS_START_TIME = new Date();

	public static void main(String[] args)
	{
		Start();
	}
	
	

	public static void Start()
	{
		log.info("系统启动.");
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
			ConsoleMgr.getInstance(SystemConfig.getInstance().getCollectPort()).start();
		}
		catch (Exception e)
		{
			log.error("采集系统启动失败,原因: 控制台模块启动异常. ", e);
		}
		
		SelfTest st = SelfTest.getInstance();
		if(!st.IsOK())
		{
			log.error("系统退出. 原因:程序自检异常");
			return;
		}

		Util.printEnvironmentInfo();

		PBeanMgr.getInstance();

		//AlarmMgr.getInstance();
		
		//ResourceManagerment.getInstance().start();

		//ProcessStatus.getInstance().start();
		
		DataLifecycleMgr.getInstance().start();

		//启动路测汇总线程
		if(SystemConfig.getInstance().isEnableRunDTStatistic())
		{
			//反射
			try
			{
				appinterface appdt = ((appinterface)Class.forName("DT.DTMain").newInstance()).getInstance();
				appdt.StartApp();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		
//		if(SystemConfig.getInstance().IsStartTaurusSocket())
//		{//Socket for taurus
//			try {
//				
//				FlexAuthServer.getInstance().startServer();
//				MonitorServer.getInstance().start();
//				
//			} catch (Exception e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
		
//		specialapp.ScanLog scanlog = specialapp.ScanLog.getInstance();
//		scanlog.startScan();
		
		ScanThread scanThread = ScanThread.getInstance();
		scanThread.startScan();		
		
		
		
		//PersistentScanThread PscanThread = PersistentScanThread.getInstance();
		//PscanThread.startScan();		
	}
	

	public String toString()
	{
		return "Capricorn V2";
	}
}