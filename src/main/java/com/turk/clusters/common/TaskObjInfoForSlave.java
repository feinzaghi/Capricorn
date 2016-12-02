package com.turk.clusters.common;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;

import com.turk.clusters.master.TaskManage;
import com.turk.clusters.model.TaskMsg;
import com.turk.clusters.slave.SlaveConfig;
import com.turk.config.ConstDef;
import com.turk.socket.Client;

import net.sf.json.JSONObject;

import com.turk.task.CollectObjInfo;
import com.turk.task.TaskMgr;
import com.turk.util.Util;

public class TaskObjInfoForSlave extends CollectObjInfo
implements Serializable {
	
private static final long serialVersionUID = 7786131247745284757L;
	
	protected HashMap<String,String> objMap = new HashMap<String, String>();
	
	private int _msgid;
	
	public void setMsgID(int msgid)
	{
		this._msgid = msgid;
	}
	
	public int getMsgID()
	{
		return this._msgid;
	}
	
	public TaskObjInfoForSlave()
  	{
  		super();
  	}
	
	public TaskObjInfoForSlave(int taskid)
  	{
  		super(taskid);
  	}
  		

  	public TaskObjInfoForSlave(int taskID, String server, int port) {
		// TODO Auto-generated constructor stub
  		super(taskID,server,port);
	}

  	
  	/**
  	 * 构建采集任务
  	 * @param rs
  	 * @param scantime
  	 * @throws Exception
  	 */
  	public void buildObj(HashMap<String,String> rs, Date scantime) throws Exception
  	{
  		//if (TaskMgr.getInstance().isActive(Integer.parseInt(rs.get("task_id")), false))
  		//{
  		//	this.log.debug(this.sysName + " is active");
  		//	return;
  		//}

  		buildObj(rs);

  		//if (checkDataTime())
  		{
  			addTask(scantime);
  		}
  	}

  	
  	
  	/**
  	 * 添加采集任务
  	 * @param scantime
  	 */
  	protected void addTask(Date scantime)
  	{
  		//log.debug("加载任务["+this.getTaskID()+"]判断是否满足时间条件满足时间条件");
  		//Calendar cal = Calendar.getInstance();
  		//int minutes = cal.get(12);
  		//int hours = cal.get(11);
  		/*
  		boolean bAdd = false;
  		int time = -1;

  		switch (getPeriod())
  		{
	  		case 1:
	  			bAdd = true;
	  			break;
		  	case 3://小时
		  	case 6:
		  	case 9://一分钟
		  	case 8:
		  		time = minutes;
		  		break;
		  	case 2://天
		  		time = hours;
		  		break;
		  	case 4://半小时
		  		time = minutes % 30;
		  		break;
		  	case 5://15分钟
		  		time = minutes % 15;
		  		break;
		  	case 7://5分钟
		  		time = minutes % 5;
		  		break;
		  	case 10://10分钟
		  		time = minutes % 10;
		  		break;
		  	default:
		  		this.log.debug(this.sysName + " : without period type.");
		  		return;
  		}	

  		if (time != -1) {
  			bAdd = isReady(time, scantime.getTime());
  			
  			log.debug("是否满足时间条件:"+bAdd +"-"+this.getTaskID()
  					+ "["+ Util.getDateString_Standard_ss(scantime) +"]"
  					+ " ["+ Util.getDateString(this.getLastCollectTime()) +"] ");
  		}*/
  		//if (bAdd)
  		{
  			try
  			{
  				startTask();
  			}
  			catch(Exception ex)
  			{
  				log.error("Send Task Error",ex);
  			}
	  	}
  		
  	}
  	
  	
  	public boolean doAfterCollect()
  	{
  		TaskMsg task = new TaskMsg();
		task.setMsgID(2002);
		task.setTaskID(this.getTaskID());
		
		//新创建对象，需要将当前任务运行的时间传递给master
		if(!this.objMap.containsKey("suc_data_time"))
			this.objMap.put("suc_data_time", Util.getDateString_Standard_ss(new Date(this.lastCollectTime.getTime())));
		task.setObjMap(this.objMap);
		
  		JSONObject jsonObject = JSONObject.fromObject(task);
		log.debug("2002-MSG:任务完成：" + jsonObject.toString());
	    Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
	    		SlaveConfig.getInstance().getMasterPort());
	    String Result = clt.SendMsg(jsonObject.toString());
	    if(Result.equals("Done"))
	    {
	    	log.debug("2002-MSG:["+this.getTaskID()+"] Complete!");
	    	//SlaveInfo slave = TaskManage.getInstance().getSlaves(this.server);
	    	//if(slave!=null)
	    	//	slave.setCurrentCltCount(slave.getCurrentCltCount()+1);
	    	this.objMap.clear();
	  		this.objMap = null;
	    }
  		
	    TaskMgr.getInstance().removeTask(this.getTaskID(),this.getLastCollectTime().getTime());
  		
  		
  		if(getPeriod()==1)
  			return true;//实时采集
  		
  		
  		//通知主节点任务完成
  		long lastCollectTime = getLastCollectTime().getTime();
  		//采集完成后，记录信息到任务结果表
  		return setResultTimePos(getTaskID(), new Timestamp(lastCollectTime), 0);
  		
  	}
}
