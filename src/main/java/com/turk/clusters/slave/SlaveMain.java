package com.turk.clusters.slave;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.turk.clusters.model.Register;
import com.turk.socket.Client;
import com.turk.parser.taurus.MapImsiMsisdn;
import com.turk.alarm.AlarmMgr;
import com.turk.util.LogMgr;
import com.turk.config.SystemConfig;
import com.turk.console.ConsoleMgr;
import com.turk.specialapp.taurus.utele.FlexAuthServer;
import com.turk.specialapp.taurus.utele.MonitorServer;
import com.turk.util.Util;

import net.sf.json.JSONObject;

import com.turk.framework.DataLifecycleMgr;
import com.turk.framework.PBeanMgr;
import com.turk.framework.SelfTest;
import com.turk.framework.Version;



/**
 * �ڵ����̨
 * @author Administrator
 *
 */
public class SlaveMain {

	private static SlaveMain _instance;
	SlaveConsole console = null;
	private static Logger log = LogMgr.getInstance().getAppLogger("slave");
	
	public static void main(String[] args)
	{

		log.info("Slave ����.");
		
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
			int port = SystemConfig.getInstance().getCollectPort();
			ConsoleMgr.getInstance(port).start();
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

		AlarmMgr.getInstance();
		
		//ResourceManagerment.getInstance().start();

		DataLifecycleMgr.getInstance().start();
		
		if(SystemConfig.getInstance().IsStartTaurusSocket())
		{//Socket for taurus
			try {
				
				FlexAuthServer.getInstance().startServer();
				MonitorServer.getInstance().start();
				
				MapImsiMsisdn.getInstance();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		SlaveMain.getInstance();
	}
		
	public static synchronized SlaveMain getInstance()
	{
		if (_instance == null)
		{
			_instance = new SlaveMain();
		}
		return _instance;
	}
	
	public SlaveMain()
	{
		//��������
		console = new SlaveConsole();
		try {
			console.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("Slave �����쳣",e);
		}
		
		NodeRegister();
		
		SlaveActive.getInstance().start();
	}
	
	/**
	 * �����ڵ�ע��
	 */
	public void NodeRegister()
	{
		Register reg = new Register();
		reg.setMsgID(1001);
		reg.setServer(SlaveConfig.getInstance().getSlaveServer());
		reg.setPort(SlaveConfig.getInstance().getSlavePort());
	    reg.setMaxActiveTask(SystemConfig.getInstance().getMaxCltCount());
	    reg.setMaxCltCount(SystemConfig.getInstance().getMaxThread());
	    reg.setFlag(SlaveConfig.getInstance().getSlaveFlag());
	    
		JSONObject jsonObject = JSONObject.fromObject(reg);
		log.debug("1001-MSG:" + jsonObject.toString());
	    Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
	    		SlaveConfig.getInstance().getMasterPort());
	    String Result = clt.SendMsg(jsonObject.toString());
	    if(Result.equals("Done"))
	    {
	    	log.debug("1001-MSG:Slave["+reg.getServer()+"]ע��ɹ�");
	    }
	    
	}
}
