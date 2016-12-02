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
 * Capricorn V2 ƽ̨�����
 * @author 
 * Capricorn(Ħ����)
 *   �й��˵���������־�ᶨ����ʱ���������θС�����Ȩ����������
 *   ���쵼ͳ������һ�ף��Գ�һ����֯����Ҳ�ܲ���
 *   
 * V2�汾˵����
 *    V2��Դ��V1�汾�����������汾�������˲ɼ������ܣ������˲ɼ�ά����Ч��
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
		log.info("ϵͳ����.");
		Version ver = Version.getInstance();
		String strVer = ver.getExpectedVersion();
		if (!ver.isRightVersion())
		{
			log.error("ϵͳ�˳�. ԭ��:�汾�Ų�һ��. �ڲ��汾��:" + strVer);
			return;
		}
		log.info("�汾��:" + strVer);
		try
		{
			ConsoleMgr.getInstance(SystemConfig.getInstance().getCollectPort()).start();
		}
		catch (Exception e)
		{
			log.error("�ɼ�ϵͳ����ʧ��,ԭ��: ����̨ģ�������쳣. ", e);
		}
		
		SelfTest st = SelfTest.getInstance();
		if(!st.IsOK())
		{
			log.error("ϵͳ�˳�. ԭ��:�����Լ��쳣");
			return;
		}

		Util.printEnvironmentInfo();

		PBeanMgr.getInstance();

		//AlarmMgr.getInstance();
		
		//ResourceManagerment.getInstance().start();

		//ProcessStatus.getInstance().start();
		
		DataLifecycleMgr.getInstance().start();

		//����·������߳�
		if(SystemConfig.getInstance().isEnableRunDTStatistic())
		{
			//����
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