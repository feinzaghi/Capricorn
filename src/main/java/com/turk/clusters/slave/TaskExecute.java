package com.turk.clusters.slave;

import java.util.Date;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.turk.clusters.common.ReTaskObjInfo;
import com.turk.clusters.common.TaskObjInfoForSlave;
import com.turk.clusters.model.TaskMsg;
import com.turk.socket.Client;
import com.turk.util.LogMgr;

public class TaskExecute implements IExecute{

	private Logger log = LogMgr.getInstance().getAppLogger("slave");
	private Logger errorlog = LogMgr.getInstance().getErrorLogger();
	
	
	/**
	 * ִ�вɼ�
	 * @param info
	 */
	public String Execute(String msgBody)
	{
		try {
			TaskMsg info = new TaskMsg();
			info = info.getByJson(msgBody);
	    	if(info != null)
	    	{
				//��������
				if(info.getIsReCLT() == 0)
				{//�����ɼ�����
					TaskObjInfoForSlave obj = new TaskObjInfoForSlave();
					obj.buildObj(info.getObjMap(), new Date());
					log.debug("Slave:Task-Start:[" + obj.getTaskID() + "]-[" + obj.getLastCollectTime() +"]");
				}
				else
				{//��������
					int id = Integer.parseInt(info.getObjMap().get("id"));
					int taskid = Integer.parseInt(info.getObjMap().get("taskid"));
					ReTaskObjInfo obj = new ReTaskObjInfo(id,taskid);
					obj.buildObj(info.getObjMap(), new Date());
					log.debug("Slave:RE-Task-Start:[" + obj.getTaskID() + "]-[" + obj.getLastCollectTime() +"]");
				}  
				
				log.debug("Task:[" + info.getTaskID() +"] Start....");	
	    		TaskMsg task = new TaskMsg();
		  		task.setMsgID(2003);//֪ͨ���أ������Ѽ����б������ش������б���ɾ��������
		  		task.setTaskID(info.getTaskID());
		  		task.setObjMap(info.getObjMap());
		  	  	JSONObject json = JSONObject.fromObject(task);
		  		    //System.out.println(jsonObject);
		  		log.debug("2003-MSG:�����Ѽ����б�" + json.toString());
		  		Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
		  		    		SlaveConfig.getInstance().getMasterPort());
		  		String Result = clt.SendMsgNetty(json.toString());
		  		if(Result.equals("Done"))
		  		{
		  		    log.debug("2003-MSG:["+info.getTaskID()+"] Complete!");
		  		}
	    	}
			return "Done";
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			errorlog.error("Slave Exec Error",e);
			return "ERROR";
		}
	}


	


	
	
}
