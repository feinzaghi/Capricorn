package com.turk.framework;

import java.io.File;

import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.util.LogMgr;

/**
 * �Լ�ģ��
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
		//���ɼ�Ŀ¼Data�����Ƿ���ȷ
		File fData = new File(SystemConfig.getInstance().getCurrentPath());
		if(fData.exists())
		{
			log.debug("���ɼ����ݴ��Ŀ¼�Ƿ����...OK!");
			IsOk = true;
		}
		else
		{
			log.debug("���ɼ����ݴ��Ŀ¼�Ƿ����...Failure!");
			IsOk = false;
		}
		
		File fTemplate = new File(SystemConfig.getInstance().getTempletPath());
		if(fTemplate.exists())
		{
			log.debug("���ɼ�����ģ��Ŀ¼�Ƿ����...OK!");
			IsOk = true;
		}
		else
		{
			log.debug("���ɼ�����ģ��Ŀ¼�Ƿ����...Failure!");
			IsOk = false;
		}
		
		return IsOk;
	}
}
