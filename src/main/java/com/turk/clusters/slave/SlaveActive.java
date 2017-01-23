package com.turk.clusters.slave;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.turk.clusters.model.Register;
import com.turk.config.SystemConfig;
import com.turk.socket.Client;
import com.turk.util.LogMgr;
import com.turk.util.ThreadPool;

/**
 * 节点报活
 * 1分钟报告一次
 * @author Administrator
 *
 */
public class SlaveActive implements Runnable{

	private static SlaveActive instance;
	private boolean stopFlag = false;
	private Logger logger = LogMgr.getInstance().getAppLogger("slave");
	private Thread thread = new Thread(this, toString());
	
	public static synchronized SlaveActive getInstance()
	{
		if (instance == null)
    	{
			instance = new SlaveActive();
    	}
		return instance;
	}
	
	public void start()
	{
		this.thread.start();
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		while (!isStop())
		{
			try
			{
				Register reg = new Register();
				reg.setMsgID(1002); //报活同步消息
				reg.setServer(SlaveConfig.getInstance().getSlaveServer());
				reg.setPort(SlaveConfig.getInstance().getSlavePort());
				reg.setCurrentCltCount(ThreadPool.getInstance().ActiveTaskCount());
				reg.setMaxActiveTask(SystemConfig.getInstance().getMaxCltCount());
				reg.setMaxCltCount(SystemConfig.getInstance().getMaxThread());
				reg.setFlag(SlaveConfig.getInstance().getSlaveFlag());

				JSONObject jsonObject = JSONObject.fromObject(reg);
			    //System.out.println(jsonObject);
				logger.debug("1002-MSG：" + jsonObject.toString());
			    Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
			    		SlaveConfig.getInstance().getMasterPort());
			    String Result = clt.SendMsgNetty(jsonObject.toString());
			    if(Result.equals("Done"))
			    {
			    	logger.debug("1002-MSG-["+reg.getServer()+"] send success!");
			    }
				Thread.sleep(5*1000L); //5s报告一次
			}
			catch (InterruptedException ie)
			{
				this.logger.warn("外界强行中断.");
				this.stopFlag = true;
				break;
			}
			catch (Exception e)
			{
				this.logger.error(this + ": 异常.原因:", e);
				break;
			}
		}
		this.logger.debug("节点自动报活扫描线束");
	}

	public synchronized boolean isStop()
	{
		return this.stopFlag;
	}
	
	public synchronized boolean Stop()
	{
		this.stopFlag = true;
		Register reg = new Register();
		reg.setMsgID(1003); //报活同步消息
		reg.setServer(SlaveConfig.getInstance().getSlaveServer());
		reg.setPort(SlaveConfig.getInstance().getSlavePort());
		reg.setCurrentCltCount(ThreadPool.getInstance().ActiveTaskCount());
		reg.setMaxActiveTask(SystemConfig.getInstance().getMaxCltCount());
		reg.setMaxCltCount(SystemConfig.getInstance().getMaxThread());
		reg.setFlag(3);

		JSONObject jsonObject = JSONObject.fromObject(reg);
	    //System.out.println(jsonObject);
		logger.debug("1003-MSG：" + jsonObject.toString());
	    Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
	    		SlaveConfig.getInstance().getMasterPort());
	    String Result = clt.SendMsg(jsonObject.toString());
	    if(Result.equals("Done"))
	    {
	    	logger.debug("1003-MSG-["+reg.getServer()+"] send success!");
	    	return true;
	    }
	    return false;
	    
	}
}
