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
 * Master ���ڵ�
 * @author Administrator
 *
 */
public class MasterMain {
	
	private static Logger log = LogMgr.getInstance().getAppLogger("master");
	
	public static void main(String[] args)
	{
		
		log.info("Master ����.");
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
			//�������̨
			int port = 9020;
			ConsoleMgr.getInstance(port).start();
		}
		catch (Exception e)
		{
			log.error("�ɼ�ϵͳ����ʧ��,ԭ��: ����̨ģ�������쳣. ", e);
		}
		
		//�Լ�ģ��
		SelfTest st = SelfTest.getInstance();
		if(!st.IsOK())
		{
			log.error("ϵͳ�˳�. ԭ��:�����Լ��쳣");
			return;
		}

		//��ӡ��������
		Util.printEnvironmentInfo();

		//����bean
		PBeanMgr.getInstance();

		//AlarmMgr.getInstance();
		
		//ResourceManagerment.getInstance().start();

		//DataLifecycleMgr.getInstance().start();

		//���ڵ�
		MasterMain.getInstance();
		
		//�ɼ�����ģ��
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
			log.error("Master �����쳣",e);
		}
	}
}
