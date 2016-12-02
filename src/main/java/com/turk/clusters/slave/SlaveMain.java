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
 * 节点控制台
 * @author Administrator
 *
 */
public class SlaveMain {

	private static SlaveMain _instance;
	SlaveConsole console = null;
	private static Logger log = LogMgr.getInstance().getAppLogger("slave");
	
	public static void main(String[] args)
	{

		log.info("Slave 启动.");
		
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
			int port = SystemConfig.getInstance().getCollectPort();
			ConsoleMgr.getInstance(port).start();
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
		//启动监听
		console = new SlaveConsole();
		try {
			console.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("Slave 启动异常",e);
		}
		
		NodeRegister();
		
		SlaveActive.getInstance().start();
	}
	
	/**
	 * 在主节点注册
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
	    	log.debug("1001-MSG:Slave["+reg.getServer()+"]注册成功");
	    }
	    
	}
}
