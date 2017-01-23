package com.turk.clusters.slave;

import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.clusters.common.ReTaskObjInfo;
import com.turk.clusters.common.TaskObjInfoForSlave;
import com.turk.clusters.model.TaskMsg;
import com.turk.util.LogMgr;

public class TaskExecute implements IExecute{

	private Logger log = LogMgr.getInstance().getAppLogger("slave");
	private Logger errorlog = LogMgr.getInstance().getErrorLogger();
	
	
	/**
	 * ִ�вɼ�
	 * @param info
	 */
	public boolean Execute(Object object)
	{
		try {
			
			TaskMsg info = (TaskMsg)object;
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
			  	
			return true;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			errorlog.error("Slave Exec Error",e);
			return false;
		}
	}


	


	
	
}
