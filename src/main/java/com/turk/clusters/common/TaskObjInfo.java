package com.turk.clusters.common;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import com.turk.clusters.master.TaskManage;
import com.turk.clusters.model.SlaveInfo;
import com.turk.clusters.model.TaskMsg;
import com.turk.clusters.slave.SlaveConfig;
import com.turk.config.ConstDef;
import com.turk.socket.Client;

import net.sf.json.JSONObject;

import com.turk.task.CollectObjInfo;
import com.turk.task.TaskMgr;
import com.turk.util.Util;

public class TaskObjInfo extends CollectObjInfo
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
	
	public TaskObjInfo()
  	{
  		super();
  	}
	
	public TaskObjInfo(int taskid)
  	{
  		super(taskid);
  	}
  		

  	public TaskObjInfo(int taskID, String server, int port) {
		// TODO Auto-generated constructor stub
  		super(taskID,server,port);
	}

  	public boolean doAfterCollect()
  	{
  		TaskMsg task = new TaskMsg();
		task.setMsgID(2002);
		task.setTaskID(this.getTaskID());
		
		//�´���������Ҫ����ǰ�������е�ʱ�䴫�ݸ�master
		if(!this.objMap.containsKey("suc_data_time"))
			this.objMap.put("suc_data_time", Util.getDateString_Standard_ss(new Date(this.lastCollectTime.getTime())));
		task.setObjMap(this.objMap);
		
  		JSONObject jsonObject = JSONObject.fromObject(task);
		log.debug("2002-MSG:������ɣ�" + jsonObject.toString());
	    Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
	    		SlaveConfig.getInstance().getMasterPort());
	    String Result = clt.SendMsg(jsonObject.toString());
	    if(Result.equals("Done"))
	    {
	    	log.debug("2002-MSG:["+this.getTaskID()+"] Complete!");
	    	//SlaveInfo slave = TaskManage.getInstance().getSlaves(this.server);
	    	//if(slave!=null)
	    	//	slave.setCurrentCltCount(slave.getCurrentCltCount()+1);
	    }
  		
	    TaskMgr.getInstance().removeTask(this.getTaskID(),this.getLastCollectTime().getTime());
  		
  		
  		if(getPeriod()==1)
  			return true;//ʵʱ�ɼ�
  		
  		
  		//֪ͨ���ڵ��������
  		long lastCollectTime = getLastCollectTime().getTime();
  		//�ɼ���ɺ󣬼�¼��Ϣ����������
  		return setResultTimePos(getTaskID(), new Timestamp(lastCollectTime), 0);
  		
  	}
  	
  	/**
  	 * �ڵ�������������ڵ��ɷ�����
  	 * ������2001
  	 */
  	public void SendTask()
  	{
  		//�ж������Ƿ���ڣ�����������б���
  		if (TaskManage.getInstance().addTask(this))
  		{
  			TaskMsg task = new TaskMsg();
  			task.setMsgID(2001);
  			task.setTaskID(this.getTaskID());
  			task.setIsReCLT(0);
  			
  			task.setObjMap(this.objMap);
  			
	  		JSONObject jsonObject = JSONObject.fromObject(task);
			log.debug("2001-MSG��" + jsonObject.toString());
		    Client clt = new Client(this.server,
		    		this.port);
		    String Result = clt.SendMsg(jsonObject.toString());
		    if(Result.equals("Done"))
		    {
		    	log.debug("2001-MSG:����["+this.getTaskID() + "#" + Util.getDateString(this.getLastCollectTime()) +"]���ͳɹ�");
		    }
		    /*else if(Result.equals("NO"))
		    {
		    	//�̶߳�����������������
		    	TaskManage.getInstance().delActiveTask(this.getTaskID(), false);
		    	log.debug("2001-MSG:����["+this.getTaskID() + "#" + Util.getDateString(this.getLastCollectTime()) +"]����ʧ�ܣ�ԭ�򣺽ڵ��̳߳��ѳ���100������");
		    	
		    }*/
		    else
		    {
		    	//���û�з��ͳɹ����ӵ�ǰ�б���ɾ�����񣬴���һ�η���
		    	TaskManage.getInstance().delActiveTask(this.getTaskID(), false);
		    	//�ѵ�ǰ����ʧ�ܵķ��������Ϊ�쳣
		    	SlaveInfo slave = TaskManage.getInstance().getSlaves().get(this.server);
		    	if(slave!=null)
		    		slave.setStatus(0);
		    }
		    this.objMap.clear();
  		}
  		else
  		{
  			this.objMap.clear();
  		}
  	}
  	
  	
  	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @param scantime
  	 * @throws Exception
  	 */
  	public void buildObj(ResultSet rs, Date scantime) throws Exception
  	{
  		if (TaskManage.getInstance().isActive(rs.getInt("TASK_ID"), false))
  		{
  			this.log.debug(this.sysName + " is active");
  			return;
  		}

  		//System.out.println(new Date());
  		log.debug("����������Ϣ��hashmap�У�"+ this.getTaskID());
  		buildObj(rs);
  		//System.out.println(new Date());
  	  
  		//slave�ɼ�����Ҫ��֤ʱ�䣬�������ڵ�ַ���ʱ��ɼ�
  		if (checkDataTime())
  		{
  			addTask(scantime);
  		}
  	}
  	
  
  	
  	/**
  	 * ��Ӳɼ�����
  	 * @param scantime
  	 */
  	protected void addTask(Date scantime)
  	{
  		log.debug("��������["+this.getTaskID()+"]�ж��Ƿ�����ʱ����������ʱ������");
  		Calendar cal = Calendar.getInstance();
  		int minutes = cal.get(12);
  		int hours = cal.get(11);
  		
  		boolean bAdd = false;
  		int time = -1;

  		switch (getPeriod())
  		{
	  		case 1:
	  			bAdd = true;
	  			break;
		  	case 3://Сʱ
		  	case 6:
		  	case 9://һ����
		  	case 8:
		  		time = minutes;
		  		break;
		  	case 2://��
		  		time = hours;
		  		break;
		  	case 4://��Сʱ
		  		time = minutes % 30;
		  		break;
		  	case 5://15����
		  		time = minutes % 15;
		  		break;
		  	case 7://5����
		  		time = minutes % 5;
		  		break;
		  	case 10://10����
		  		time = minutes % 10;
		  		break;
		  	default:
		  		this.log.debug(this.sysName + " : without period type.");
		  		return;
  		}	

  		if (time != -1) {
  			bAdd = isReady(time, scantime.getTime());
  			
  			log.debug("�Ƿ�����ʱ������:"+bAdd +"-"+this.getTaskID()
  					+ "["+ Util.getDateString_Standard_ss(scantime) +"]"
  					+ " ["+ Util.getDateString(this.getLastCollectTime()) +"] ");
  		}
  		if (bAdd)
  		{
  			try
  			{
  				SendTask();
  			}
  			catch(Exception ex)
  			{
  				log.error("Send Task Error",ex);
  			}
	  	}
  		this.objMap.clear();
  		this.objMap = null;
  	}
  	
  	
  	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @throws Exception
  	 */
  	protected void buildObj(ResultSet rs)
  		throws Exception
  	{
  		
  		objMap.put("group_id", String.valueOf(rs.getInt("GROUP_ID")));
  		objMap.put("task_describe", rs.getString("Task_Describe")==null?"":rs.getString("Task_Describe"));
  		objMap.put("task_id", String.valueOf(rs.getInt("TASK_ID")));
  		
  		objMap.put("deviceid", String.valueOf(rs.getInt("DEVICEID")));
  		objMap.put("dev_name", rs.getString("DEV_NAME")==null?"":rs.getString("DEV_NAME"));
  		objMap.put("host_ip", rs.getString("HOST_IP")==null?"":rs.getString("HOST_IP"));
  		objMap.put("host_user", rs.getString("HOST_USER")==null?"":rs.getString("HOST_USER"));
  		objMap.put("host_pwd", rs.getString("HOST_PWD")==null?"":rs.getString("HOST_PWD"));
  		objMap.put("host_sign", rs.getString("HOST_SIGN")==null?"":rs.getString("HOST_SIGN"));
  		objMap.put("encode", rs.getString("ENCODE")==null?"":rs.getString("ENCODE"));
  		objMap.put("devicename", rs.getString("DEVICENAME")==null?"":rs.getString("DEVICENAME"));
  		objMap.put("city_id", String.valueOf(rs.getInt("CITY_ID")));
  		objMap.put("vendor", rs.getString("vendor")==null?"":rs.getString("vendor"));

  		objMap.put("dbdriver", rs.getString("DBDRIVER")==null?"":rs.getString("DBDRIVER"));
  		objMap.put("dburl", rs.getString("DBURL")==null?"":rs.getString("DBURL"));
  		objMap.put("dev_port", String.valueOf(rs.getInt("DEV_PORT")));
	
	    
  		objMap.put("proxy_dev_id", String.valueOf(rs.getInt("PROXY_DEV_ID")));
  		objMap.put("proxy_dev_name", rs.getString("PROXY_DEV_NAME")==null?"":rs.getString("PROXY_DEV_NAME"));
  		objMap.put("proxy_host_ip", rs.getString("PROXY_HOST_IP")==null?"":rs.getString("PROXY_HOST_IP"));
  		objMap.put("proxy_host_user", rs.getString("PROXY_HOST_USER")==null?"":rs.getString("PROXY_HOST_USER"));
  		objMap.put("proxy_host_pwd", rs.getString("PROXY_HOST_PWD")==null?"":rs.getString("PROXY_HOST_PWD"));
  		objMap.put("proxy_host_sign", rs.getString("PROXY_HOST_SIGN")==null?"":rs.getString("PROXY_HOST_SIGN"));
  		
  		objMap.put("indbserver", rs.getString("INDBSERVER")==null?"":rs.getString("INDBSERVER"));
  		objMap.put("indbuser", rs.getString("INDBUSER")==null?"":rs.getString("INDBUSER"));
  		objMap.put("indbpassword", rs.getString("INDBPASSWORD")==null?"":rs.getString("INDBPASSWORD"));
  		
  		objMap.put("proxy_dev_port", String.valueOf(rs.getInt("PROXY_DEV_PORT")));
  		objMap.put("collect_type", String.valueOf(rs.getInt("COLLECT_TYPE")));
  		objMap.put("collecttimeout", String.valueOf(rs.getInt("CollectTimeOut")));
  		objMap.put("collect_period", String.valueOf(rs.getInt("COLLECT_PERIOD")));
  		objMap.put("collect_time", String.valueOf(rs.getInt("COLLECT_TIME")));
  		objMap.put("collect_timepos", String.valueOf(rs.getInt("COLLECT_TIMEPOS")));
  		objMap.put("shell_cmd_prepare", rs.getString("SHELL_CMD_PREPARE")==null?"":rs.getString("SHELL_CMD_PREPARE"));
  		objMap.put("shell_cmd_finish", rs.getString("SHELL_CMD_FINISH")==null?"":rs.getString("SHELL_CMD_FINISH"));
	    

  		objMap.put("parserid", String.valueOf(rs.getInt("PARSERID")));
  		objMap.put("distributorid", String.valueOf(rs.getInt("DISTRIBUTORID")));
  		objMap.put("redo_time_offset", String.valueOf(rs.getInt("REDO_TIME_OFFSET")));
  		objMap.put("prob_starttime", String.valueOf(rs.getInt("prob_starttime")));
  		
  		objMap.put("collector_name", rs.getString("COLLECTOR_NAME"));
  		
  		objMap.put("end_data_time", rs.getTimestamp("end_data_time")==null?"":Util.getDateString_Standard_ss(rs.getTimestamp("end_data_time")));
  		
  		if (Util.isOracle())
	    {
	    	objMap.put("collect_path", ConstDef.ClobParse(rs.getClob("COLLECT_PATH")));
	    }
	    else if (Util.isSybase())
	    {
	    	objMap.put("collect_path", rs.getString("COLLECT_PATH")==null?"":rs.getString("COLLECT_PATH"));
	    }
	    else if (Util.isMySQL())
	    {
	    	objMap.put("collect_path", rs.getString("COLLECT_PATH")==null?"":rs.getString("COLLECT_PATH"));
	    }

	    objMap.put("shell_timeout", String.valueOf(rs.getInt("SHELL_TIMEOUT")));
    

	    objMap.put("parse_tmpid", String.valueOf(rs.getInt("PARSE_TMPID")));
	    objMap.put("tmptype_p", String.valueOf(rs.getInt("TMPTYPE_P")));
	    objMap.put("tmpname_p", rs.getString("TMPNAME_P")==null?"":rs.getString("TMPNAME_P"));
	    objMap.put("edition_p", rs.getString("EDITION_P")==null?"":rs.getString("EDITION_P"));
	    objMap.put("tempfilename_p", rs.getString("TEMPFILENAME_P")==null?"":rs.getString("TEMPFILENAME_P"));
	    
	    objMap.put("distrbute_tmpid", String.valueOf(rs.getInt("DISTRBUTE_TMPID")));
	    objMap.put("tmptype_d", String.valueOf(rs.getInt("TMPTYPE_D")));
	    objMap.put("tmpname_d", rs.getString("TMPNAME_D")==null?"":rs.getString("TMPNAME_D"));
	    objMap.put("edition_d", rs.getString("EDITION_D")==null?"":rs.getString("EDITION_D"));
	    objMap.put("tempfilename_d", rs.getString("TEMPFILENAME_D")==null?"":rs.getString("TEMPFILENAME_D"));
	    
	    objMap.put("suc_data_time", rs.getTimestamp("SUC_DATA_TIME")==null?"":Util.getDateString_Standard_ss(rs.getTimestamp("SUC_DATA_TIME")));
	    
	    objMap.put("suc_data_pos", String.valueOf(rs.getInt("SUC_DATA_POS")));
	    objMap.put("maxclttime", String.valueOf(rs.getInt("MAXCLTTIME")));
	    objMap.put("threadsleeptime", String.valueOf(rs.getInt("THREADSLEEPTIME")));
	    objMap.put("blockedtime", String.valueOf(rs.getInt("BLOCKEDTIME")));

	    buildObj(objMap);
  	}
}
