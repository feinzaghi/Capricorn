package com.turk.clusters.model;

import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.clusters.master.TaskManage;
import com.turk.clusters.slave.SlaveConfig;
import com.turk.socket.Client;

import com.turk.task.TaskMgr;
import com.turk.util.LogMgr;
import net.sf.json.JSONObject;

/**
 * ��������
 * @author Administrator
 *
 */
public class StatTaskInfo extends AbstractMsg{
	
	protected Logger log = LogMgr.getInstance().getSystemLogger();

	private int _id;
	
	public int getID()
	{
		return this._id;
	}
	
	public void setID(int id)
	{
		this._id = id;
	}
	
	private String _taskname;
	
	public String getTaskName()
	{
		return this._taskname;
	}
	
	public void setTaskName(String taskname)
	{
		this._taskname = taskname;
	}

	private String _starttime;
	
	public String getStartTime()
	{
		return this._starttime;
	}
	
	public void setStartTime(String starttime)
	{
		this._starttime = starttime;
	}
	
	private String _ftpip;
	
	public String getFTPIP()
	{
		return this._ftpip;
	}
	
	public void setFTPIP(String ftpip)
	{
		this._ftpip = ftpip;
	}

	private String _ftppath;
	
	public String getFTPPath()
	{
		return this._ftppath;
	}
	
	public void setFTPPath(String ftppath)
	{
		this._ftppath = ftppath;
	}

	private String _ftpuser;
	
	public String getFTPUser()
	{
		return this._ftpuser;
	}
	
	public void setFTPUser(String ftpuser)
	{
		this._ftpuser = ftpuser;
	}

	private String _ftppwd;
	
	public String getFTPPwd()
	{
		return this._ftppwd;
	}
	
	public void setFTPPwd(String ftppwd)
	{
		this._ftppwd = ftppwd;
	}
	
	private int _tasktype;
	
	public int getTaskType()
	{
		return this._tasktype;
	}
	
	public void setTaskType(int tasktype)
	{
		this._tasktype = tasktype;
	}
	
	private int _statperiod;
	
	public int getStatPeriod()
	{
		return this._statperiod;
	}

	public void setStatPeriod(int statperiod)
	{
		this._statperiod = statperiod;
	}

	private int _stattime;
	
	/**
	 * ���������ľ���ʱ��  ����������ΪСʱ��Сʱ��������Ϊ ����
	 * @return
	 */
	public int getStatTime()
	{
		return this._stattime;
	}
	
	public void setStatTime(int stattime)
	{
		this._stattime = stattime;
	}
	
	@SuppressWarnings("static-access")
	public StatTaskInfo getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		StatTaskInfo obj = null;
		obj = (StatTaskInfo)jsonobject.toBean(jsonobject,
				StatTaskInfo.class);
		return obj;
	}
	
	
	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @param scantime
  	 * @throws Exception
  	 */
  	public void buildObj(ResultSet rs, Date scantime) throws Exception
  	{
  		if (TaskMgr.getInstance().isActive(rs.getInt("ID"), false))
  		{
  			this.log.debug(rs.getInt("TASK_NAME") + " is active");
  			return;
  		}

  		buildObj(rs);

  		if (checkDataTime())
  		{
  			addTaskItem(scantime);
  		}
  	}
  	
  	/**
  	 * ���ɼ������ʱ�䣬�Ƿ���Խ��вɼ�
  	 * @return
  	 */
  	private boolean checkDataTime()
  	{
  		return true;
  	}
  	
  	/**
  	 * ��Ӳɼ�����
  	 * @param scantime
  	 */
  	protected void addTaskItem(Date scantime)
  	{
  		Calendar cal = Calendar.getInstance();
  		int minutes = cal.get(12);
  		int hours = cal.get(11);

  		boolean bAdd = false;
  		int time = -1;

  		switch (this._statperiod)
  		{
	  		case 1:
	  			bAdd = true;
	  			break;
		  	case 3:
		  	case 6:
		  	case 8:
		  		time = minutes;
		  		break;
		  	case 2://Сʱ
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
		  	default:
		  		this.log.debug(this._taskname + " : without period type.");
		  		return;
  		}	

  		if (time != -1) {
  			bAdd = isReady(time, scantime.getTime());
  		}
  		if (bAdd)
  		{
  			startTask();
	  	}
  	}
  	
  	/**
  	 * �ڵ�������������ڵ��ɷ�����
  	 * ������2001
  	 */
  	public void startTask()
  	{
  		if (TaskManage.getInstance().addStatTask(this))
  		{
  			StatTaskInfo task = new StatTaskInfo();
  			task.setMsgID(2003);
  			task.setID(this.getID());
	  		JSONObject jsonObject = JSONObject.fromObject(task);
			log.debug("2003-MSG��" + jsonObject.toString());
		    Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
		    		SlaveConfig.getInstance().getMasterPort());
		    String Result = clt.SendMsg(jsonObject.toString());
		    if(Result.equals("Done"))
		    {
		    	log.debug("2003-MSG:����["+this._id+"]���ͳɹ�");
		    }
  		}
  	}
  	
	/**
  	 * �ж��Ƿ�����вɼ����˴����Ʋɼ���ʱ�Ͳɼ�����ʱ���
  	 * @param unit
  	 * @param scanTime
  	 * @return
  	 */
  	private boolean isReady(int unit, long scanTime)
  	{
  		try {
	  		boolean bReturn = false;
	
	  		//�ɼ���������ʱ�䣬��ɼ�ΪСʱ��Сʱ�ɼ�Ϊ����
	  		int statTime = getStatTime();
	  		statTime = statTime > 59 ? 0 : statTime;
	  		
	  		//�ɼ�����ƫ�ƶ��ٷ��ӿ�ʼ�ɼ�
	  		SimpleDateFormat ft1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	  		Date starttime;
			starttime = ft1.parse(getStartTime());
			
	  		long startTime = starttime.getTime() + 
	  			getStatTimePos() * 1000;
	
	  		if (scanTime - startTime >= getPeriodTime())
	  		{
	  			//DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(getTaskID()));
	  			bReturn = true;
	  		}
	  		else if ((unit >= statTime) && (startTime < scanTime))
	  		{
	  			//DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(getTaskID()));
	  			bReturn = true;
	  		}
	
	  		if (!bReturn)
	  		{
	  			//TaskMgr.getInstance().tempTasks.add(this);
	  		}
	  		return bReturn;
  		} catch (ParseException e) {
			// TODO Auto-generated catch block
			log.error(e);
			return false;
		}
  	}
  	
  	/**
  	 * ����ƫ��ʱ�䣨�룩
  	 * @return
  	 */
  	private long getStatTimePos()
  	{
  		long time = 0L;
  		switch (getStatPeriod())
  		{
  			case 1:
  				time = 3600000L;
  				break;
  			case 2://Сʱ���� �Ƴ�3Сʱ����̽��
  				time = 15*60;
  				break;
  			case 3://������ �Ƴ�1��+3Сʱ����̽��
  				time = 3*3600;
  				break;
  			case 6:
  				time = 14400000L;
  				break;
  			case 4:
  				time = 1800000L;
  				break;
  			case 5:
  				time = 900000L;
  				break;
  			case 7:
  				time = 300000L;
  				break;
  			case 8:
  				time = 43200000L;
  		}
  		return time;
  	}
  	
  	/**
  	 * ���ݲɼ����ڿ��Ʋɼ�ʱ���
  	 * @return
  	 */
  	private long getPeriodTime()
  	{
  		long time = 0L;
  		switch (getStatPeriod())
  		{
  			case 1:
  				time = 3600000L;
  				break;
  			case 3:
  				time = 3600000L;
  				break;
  			case 6:
  				time = 14400000L;
  				break;
  			case 2:
  				time = 86400000L;
  				break;
  			case 4:
  				time = 1800000L;
  				break;
  			case 5:
  				time = 900000L;
  				break;
  			case 7:
  				time = 300000L;
  				break;
  			case 8:
  				time = 43200000L;
  		}

  		return time;
  	}
  	
  	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @throws Exception
  	 */
  	public void buildObj(ResultSet rs) throws Exception
  	{
  		setID(rs.getInt("ID"));
  		setTaskName(rs.getString("TASK_NAME"));
  		
  		Date st = rs.getDate("START_TIME");
  		SimpleDateFormat ft1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  		setStartTime(ft1.format(st));
  		
  		setFTPIP(rs.getString("FTPIP"));
  		setFTPPath(rs.getString("FTPPATH"));
  		setFTPUser(rs.getString("FTPUSER"));
  		setFTPPwd(rs.getString("FTPPWD"));
  		setStatPeriod(rs.getInt("STAT_TYPE"));
  		setTaskType(rs.getInt("TASK_TYPE"));
  	}
}
