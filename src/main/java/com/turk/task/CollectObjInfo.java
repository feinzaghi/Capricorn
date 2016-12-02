package com.turk.task;

import com.turk.access.AbstractAccessor;
import com.turk.Config.ConstDef;
import com.turk.Config.SystemConfig;
import com.turk.framework.Factory;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.turk.datalog.DataLogInfo;
import com.turk.delayprobe.DelayProbeMgr;
import com.turk.templet.AbstractTempletBase;
import com.turk.templet.TempletBase;
import com.turk.templet.TempletRecord;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Task;
import com.turk.util.ThreadPool;
import com.turk.util.Util;

/**
 * �ɼ����������Ϣ
 * @author Administrator
 *
 */
public class CollectObjInfo
  implements Serializable
{
	private static final long serialVersionUID = 7786131247745284757L;
	protected int keyID = 0;

	private int groupId = 0;
	private String describe;
	protected int taskId = 0;
	private DevInfo devInfo;
	private int devPort = 0;
	private DevInfo proxyDevInfo;
	private int proxyDevPort = 0;
	private int collectType = 0;
	private int collectTimeOut = 0;
	private int collectPeriod = 0;
	private int collectTime = 0;
	private int collectTimePos = 0;
	protected String collectPath;
	private String shellCmdPrepare = "";
	private String shellCmdFinish = "";
	private int shellTimeOut;
	private int parseTmpID = 0;
	private int parseTmpType = 0;
	private AbstractTempletBase parseTemplet;
	private int disTmpID = 0;
	private AbstractTempletBase distributeTemplet;
	private int redoTimeOffset = 0;
	private int parserID;
  	private int distributorID;
  	protected Timestamp lastCollectTime;
  	private int lastCollectPos;
  	protected boolean usedFlag = false;
  	private int maxReCollectTime = 0;

  	private int activeTableIndex = -1;

  	private String dbDriver = "";
  	private String dbUrl = "";
  	protected Task threadHandle = null;
  	private Timestamp sqlldrTime;
  	private int threadSleepTime = 0;
  	private int blockedTime;
  	private String hostName;
  	protected Timestamp endDataTime = null;

  	protected Logger log = LogMgr.getInstance().getSystemLogger();
  	
  	protected Logger errorlog = LogMgr.getInstance().getErrorLogger();

  	private String tempTempFileName = null;
  	private TempletRecord parseTmpRecord;
  	private TempletRecord distTmpRecord;
  	protected String sysName;
  	protected int probeTime = -1;
  	private boolean isPersistentTask = false;

  	protected final DataLogInfo logInfo = new DataLogInfo();
  	public Timestamp startTime;
  	public int m_nAllRecordCount = 0;
  	
  	private InDBServer _indbserverconf;

  	protected String server;
  	protected int port;
  	
  	
  	public CollectObjInfo(int taskID)
  	{
  		this.taskId = taskID;
  		this.keyID = taskID;
  		this.sysName = String.valueOf(this.taskId);
  	}
  	
  	public CollectObjInfo(int taskID,String server,int port)
  	{
  		this.taskId = taskID;
  		this.keyID = taskID;
  		this.sysName = String.valueOf(this.taskId);
  		this.server = server;
  		this.port = port;
  	}
  	
  	public CollectObjInfo()
  	{
  		
  	}

  	/**
  	 * ����ɼ�����������
  	 * @return
  	 */
  	public String getHostName()
  	{
  		return this.hostName;
  	}

  	/**
  	 * ����ɼ�����������
  	 * @param strName
  	 */
  	public void setHostName(String strName)
  	{
  		this.hostName = strName;
  	}

  
  	/**
  	 * 
  	 * @return
  	 */
  	public Timestamp getSqlldrTime()
  	{
  		return this.sqlldrTime;
  	}

  	public void setSqlldrTime(Timestamp ts)
  	{
  		this.sqlldrTime = ts;
  	}

  	/**
  	 * ������Ĳɼ��̶߳���
  	 * @return
  	 */
  	public Task getCollectThread()
  	{
  		return this.threadHandle;
  	}

  	public void setCollectThread(Task hThreadHandle)
  	{
  		this.threadHandle = hThreadHandle;
  	}

  	/**
  	 * �ɼ���������ʱ��
  	 * @return
  	 */
  	public int getThreadSleepTime()
  	{
  		return this.threadSleepTime;
  	}

  	/**
  	 * �ɼ���������ʱ��
  	 */
  	public void setThreadSleepTime(int time)
  	{
  		this.threadSleepTime = time;
  	}

  	/**
  	 * ���ɼ�λ��
  	 * @return
  	 */
  	public int getLastCollectPos()
  	{
  		return this.lastCollectPos;
  	}

  	
  	public int getGroupId()
  	{
  		return this.groupId;
  	}

  	public void setGroupId(int id)
  	{
  		this.groupId = id;
  	}	

  	public void setDescribe(String TaskDescribe)
  	{
  		this.describe = TaskDescribe;
  	}

  	public String getDescribe()
  	{
  		return this.describe;
  	}

  	/**
  	 * �ɼ�������
  	 */
  	public void setTaskID(int nTaskId)
  	{
  		this.taskId = nTaskId;
  	}

  	/**
  	 * �ɼ�������
  	 * @return
  	 */
  	public int getTaskID()
  	{
  		return this.taskId;
  	}

  	public void setActiveTableIndex(int index)
  	{
  		this.activeTableIndex = index;
  	}

  	public int getActiveTableIndex()
  	{
  		return this.activeTableIndex;
  	}

  	public void setDevInfo(DevInfo devInfo)
  	{
  		this.devInfo = devInfo;
  	}

  	public DevInfo getDevInfo()
  	{
  		return this.devInfo;
  	}

  	public void setDevPort(int port)
  	{
  		this.devPort = port;
  	}

  	public int getDevPort()
  	{
  		return this.devPort;
  	}

  	public void setProxyDevInfo(DevInfo devInfo)
  	{
  		this.proxyDevInfo = devInfo;
  	}

  	public DevInfo getProxyDevInfo()
  	{
  		return this.proxyDevInfo;
  	}

  	public void setProxyDevPort(int port)
  	{
  		this.proxyDevPort = port;
  	}

  	public int getProxyDevPort()
  	{
  		return this.proxyDevPort;
  	}

  	public void setCollectType(int type)
  	{
  		this.collectType = type;
  	}

  	/**
  	 * �ɼ�����
  	 * @return
  	 */
  	public int getCollectType()
  	{
  		return this.collectType;
  	}

  	public void setCollectTimeOut(int timeout)
  	{
  		this.collectTimeOut = timeout;
  	}

  	/**
  	 * ��ʱʱ��(����)
  	 * @return
  	 */
  	public int getCollectTimeOut()
  	{
  		return this.collectTimeOut;
  	}

  	public void setPeriod(int period)
  	{
  		this.collectPeriod = period;
  	}

  	/**
  	 * �ɼ����� ��Сʱ����ȣ�
  	 * @return
  	 */
  	public int getPeriod()
  	{
  		return this.collectPeriod;
  	}

  	public void setCollectTime(int time)
  	{
  		this.collectTime = time;
  	}

  	/**
  	 * ����ɼ�ʱ��㣬�ɼ�����������ʱ���
  	 * @return
  	 */
  	public int getCollectTime()
  	{
  		return this.collectTime;
  	}

  	
  	public void setCollectTimePos(int offset)
  	{
  		this.collectTimePos = offset;
  	}

  	/**
  	 * �ɼ��������ʱʱ��
  	 * @return
  	 */
  	public int getCollectTimePos()
  	{
  		return this.collectTimePos;
  	}

  	public void setCollectPath(String path)
  	{
  		this.collectPath = path;
  	}

  	/**
  	 * �ɼ�·��
  	 * @return
  	 */
  	public String getCollectPath()
  	{
  		return this.collectPath;
  	}

  	public void setShellCmdPrepare(String cmd)
  	{
  		this.shellCmdPrepare = cmd;
  	}

  	/**
  	 * �ɼ�ǰ��shellָ��
  	 * @return
  	 */
  	public String getShellCmdPrepare()
  	{
  		return this.shellCmdPrepare;
  	}

  	public void setShellCmdFinish(String cmd)
  	{
  		this.shellCmdFinish = cmd;
  	}

  	/**
  	 * �ɼ���ɺ��shellָ��
  	 * @return
  	 */
  	public String getShellCmdFinish()
  	{
  		return this.shellCmdFinish;
  	}

  	/**
  	 * Shellָ��ĳ�ʱʱ��
  	 * @return
  	 */
  	public int getShellTimeout()
  	{
  		return this.shellTimeOut;
  	}

  	public void setShellTimeout(int timeout)
  	{
  		this.shellTimeOut = timeout;
  	}

  	public void setParseTmpID(int ParseTmpID)
  	{
  		this.parseTmpID = ParseTmpID;
  	}

  	/**
  	 * ����ģ����
  	 * @return
  	 */
  	public int getParseTmpID()
  	{
  		return this.parseTmpID;
  	}

  	public void setParseTmpType(int parseTmpType)
  	{
  		this.parseTmpType = parseTmpType;
  	}

  	/**
  	 * ����ģ������
  	 * @return
  	 */
  	public int getParseTmpType()
  	{
  		return this.parseTmpType;
  	}

  	public void setParseTemplet(AbstractTempletBase tmpBase)
  	{
  		this.parseTemplet = tmpBase;
  	}

  	/**
  	 * ����ģ��
  	 * @return
  	 */
  	public TempletBase getParseTemplet()
  	{
  		return this.parseTemplet;
  	}

  	public void setDistributeTemplet(AbstractTempletBase distributeTemplet)
  	{
  		this.distributeTemplet = distributeTemplet;
  	}

  	/**
  	 * �ַ�ģ��
  	 * @return
  	 */
  	public TempletBase getDistributeTemplet()
  	{
  		return this.distributeTemplet;
  	}

  	/**
  	 * ���ɼ�ʱ��/��һ���ɼ��Ŀ�ʼʱ��
  	 * @param ts
  	 */
  	public void setLastCollectTime(Timestamp ts)
  	{
  		this.lastCollectTime = ts;
  	}

  	public Timestamp getLastCollectTime()
  	{
  		return this.lastCollectTime;
  	}

  	public void setLastCollectPos(int pos)
  	{
  		this.lastCollectPos = pos;
  	}

  	public void addLastCollectPos(int nAdd)
  	{
  		this.lastCollectPos += nAdd;
  	}

  	public void setUsed(boolean isUsed)
  	{
  		this.usedFlag = isUsed;
  	}

  	/**
  	 * �����Ƿ����
  	 * @return
  	 */
  	public boolean isUsed()
  	{
  		return this.usedFlag;
  	}

  	public void setMaxReCollectTime(int nMaxReCollectTime)
  	{
  		this.maxReCollectTime = nMaxReCollectTime;
  	}

  	/**
  	 * ��󲹲ɴ���
  	 * @return
  	 */
  	public int getMaxReCollectTime()
  	{
  		return this.maxReCollectTime;
  	}

  	/**
  	 * �ɼ��Է����ݿ�����
  	 * @return
  	 */
  	public String getDBDriver()
  	{
  		return this.dbDriver;
  	}

  	public void setDBDriver(String driver)
  	{
  		this.dbDriver = driver;
  	}

  	/**
  	 * �ɼ��Է����ݿ�����
  	 * @return
  	 */
  	public String getDBUrl()
  	{
  		return this.dbUrl;
  	}

  	public void setDBUrl(String url)
  	{
  		this.dbUrl = url;
  	}

  	public int getBlockedTime()
  	{
  		return this.blockedTime;
  	}

  	public void setBlockedTime(int t)
  	{
  		this.blockedTime = t;
  	}

  	public int getKeyID()
  	{
  		return this.keyID;
  	}

  	public void setKeyID(int KeyID)
  	{	
  		this.keyID = KeyID;
  	}
  	
  	/**
  	 * ������ݿ�����
  	 * @param dbserver
  	 */
  	public void setInDBServerConfig(InDBServer dbserver)
  	{
  		_indbserverconf = dbserver;
  	}
  	
  	/**
  	 * ������ݿ�����
  	 * @return
  	 */
  	public InDBServer getInDBServerConfig()
  	{
  		return this._indbserverconf;
  	}

  	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @throws Exception
  	 */
  	public void buildData(ResultSet rs) throws Exception
  	{
  		buildObj(rs);
  	}

  	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @param scantime
  	 * @throws Exception
  	 */
  	protected void buildObj(ResultSet rs, Date scantime) throws Exception
  	{
  		if (TaskMgr.getInstance().isActive(rs.getInt("TASK_ID"), false))
  		{
  			this.log.debug(this.sysName + " is active");
  			return;
  		}

  		buildObj(rs);

  		if (checkDataTime())
  		{
  			addTaskItem(scantime);
  		}
  	}
  	
  	
  	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @param scantime
  	 * @throws Exception
  	 */
  	public void buildObj(HashMap<String,String> rs, Date scantime) throws Exception
  	{
  		if (TaskMgr.getInstance().isActive(Integer.parseInt(rs.get("task_id")), false))
  		{
  			this.log.debug(this.sysName + " is active");
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
  	protected boolean checkDataTime()
  	{
  		if (this.endDataTime == null) return true;
  		return this.lastCollectTime.getTime() <= this.endDataTime.getTime();
  	}

  	/**
  	 * �����ɼ�����
  	 * @param rs
  	 * @throws Exception
  	 */
  	protected void buildObj(ResultSet rs)
  		throws Exception
  	{
  		setGroupId(rs.getInt("GROUP_ID"));
	    setDescribe(rs.getString("Task_Describe"));
	    setTaskID(rs.getInt("TASK_ID"));
	    DevInfo devInfo = new DevInfo();
	    devInfo.setDevID(rs.getInt("DEVICEID"));
	    devInfo.setName(rs.getString("DEV_NAME"));
	    devInfo.setIP(rs.getString("HOST_IP"));
	    devInfo.setHostUser(rs.getString("HOST_USER"));
	    devInfo.setHostPwd(rs.getString("HOST_PWD"));
	    devInfo.setHostSign(rs.getString("HOST_SIGN"));
	    devInfo.setEncode(rs.getString("ENCODE"));
	    devInfo.setDeviceName(rs.getString("DEVICENAME"));
	    devInfo.setCityID(rs.getInt("CITY_ID"));
	    devInfo.setVendor(rs.getString("vendor"));
	    setDBDriver(rs.getString("DBDRIVER"));
	    setDBUrl(rs.getString("DBURL"));
	    setDevInfo(devInfo);
	    setDevPort(rs.getInt("DEV_PORT"));
	    DevInfo proxdevInfo = new DevInfo();
	    proxdevInfo.setDevID(rs.getInt("PROXY_DEV_ID"));
	    proxdevInfo.setName(rs.getString("PROXY_DEV_NAME"));
	    proxdevInfo.setIP(rs.getString("PROXY_HOST_IP"));
	    proxdevInfo.setHostUser(rs.getString("PROXY_HOST_USER"));
	    proxdevInfo.setHostPwd(rs.getString("PROXY_HOST_PWD"));
	    proxdevInfo.setHostSign(rs.getString("PROXY_HOST_SIGN"));
	    setProxyDevInfo(proxdevInfo);
	    
	    InDBServer indbserver = new InDBServer();
	    indbserver.setInDBServer(rs.getString("INDBSERVER"));
	    indbserver.setInDBUser(rs.getString("INDBUSER"));
	    indbserver.setInDBPassword(rs.getString("INDBPASSWORD"));
	    setInDBServerConfig(indbserver);

	    setProxyDevPort(rs.getInt("PROXY_DEV_PORT"));
	    setCollectType(rs.getInt("COLLECT_TYPE"));
	    setCollectTimeOut(rs.getInt("CollectTimeOut"));
	    setPeriod(rs.getInt("COLLECT_PERIOD"));
	    setCollectTime(rs.getInt("COLLECT_TIME"));
	    setCollectTimePos(rs.getInt("COLLECT_TIMEPOS"));
	    setShellCmdPrepare(rs.getString("SHELL_CMD_PREPARE"));
	    setShellCmdFinish(rs.getString("SHELL_CMD_FINISH"));

	    setParserID(rs.getInt("PARSERID")); //�������ͣ�������õĽ�����
	    setDistributorID(rs.getInt("DISTRIBUTORID"));
	
	    setRedoTimeOffset(rs.getInt("REDO_TIME_OFFSET"));
	    setProbeTime(rs.getInt("prob_starttime"));
	
	    this.endDataTime = rs.getTimestamp("end_data_time");

	    if (Util.isOracle())
	    {
	    	String strPath = ConstDef.ClobParse(rs.getClob("COLLECT_PATH"));
	    	setCollectPath(strPath);
	    }
	    else if (Util.isSybase())
	    {
	    	setCollectPath(rs.getString("COLLECT_PATH"));
	    }
	    else if (Util.isMySQL())
	    {
	    	setCollectPath(rs.getString("COLLECT_PATH"));
	    }

	    setShellTimeout(rs.getInt("SHELL_TIMEOUT"));
	    setParseTmpID(rs.getInt("PARSE_TMPID")); //��������
	    setParseTmpType(rs.getInt("TMPTYPE_P")); //����ģ������

	    this.parseTmpRecord = new TempletRecord();
	    this.parseTmpRecord.setId(rs.getInt("PARSE_TMPID")); //����ģ����
	    this.parseTmpRecord.setType(rs.getInt("TMPTYPE_P")); //����ģ������
	    this.parseTmpRecord.setName(rs.getString("TMPNAME_P")); 
	    this.parseTmpRecord.setEdition(rs.getString("EDITION_P"));
	    this.parseTmpRecord.setFileName(rs.getString("TEMPFILENAME_P"));

	    //����ģ��
	    this.parseTemplet = Factory.createTemplet(this.parseTmpRecord);
	
	    setDisTmpID(rs.getInt("DISTRBUTE_TMPID"));
	
	    this.distTmpRecord = new TempletRecord();
	    this.distTmpRecord.setId(rs.getInt("DISTRBUTE_TMPID"));
	    this.distTmpRecord.setType(rs.getInt("TMPTYPE_D"));
	    this.distTmpRecord.setName(rs.getString("TMPNAME_D"));
	    this.distTmpRecord.setEdition(rs.getString("EDITION_D"));
	    this.distTmpRecord.setFileName(rs.getString("TEMPFILENAME_D"));

	    this.distributeTemplet = Factory.createTemplet(this.distTmpRecord);
	
	    setLastCollectTime(rs.getTimestamp("SUC_DATA_TIME"));
	
	    setLastCollectPos(rs.getInt("SUC_DATA_POS"));
	
	    setMaxReCollectTime(rs.getInt("MAXCLTTIME"));
	
	    setThreadSleepTime(rs.getInt("THREADSLEEPTIME"));
	
	    setBlockedTime(rs.getInt("BLOCKEDTIME"));
  	}
  	
  	
  	/**
  	 * �����ɼ����� ����XML�����ļ�
  	 * @param taskInfo
  	 * @throws Exception
  	 */
  	public void buildObj(HashMap<String,String> taskInfo)
  		throws Exception
  	{
  		setGroupId(Integer.parseInt(taskInfo.get("group_id").isEmpty()?"0":taskInfo.get("group_id")));
	    setDescribe(taskInfo.get("task_describe"));
	    setTaskID(Integer.parseInt(taskInfo.get("task_id").isEmpty()?"0":taskInfo.get("task_id")));
	    
	    DevInfo devInfo = new DevInfo();
	    devInfo.setDevID(Integer.parseInt(taskInfo.get("deviceid").isEmpty()?"0":taskInfo.get("deviceid")));
	    devInfo.setName(taskInfo.get("dev_name"));
	    devInfo.setIP(taskInfo.get("host_ip"));
	    devInfo.setHostUser(taskInfo.get("host_user"));
	    devInfo.setHostPwd(taskInfo.get("host_pwd"));
	    devInfo.setHostSign(taskInfo.get("host_sign"));
	    devInfo.setEncode(taskInfo.get("encode"));
	    devInfo.setDeviceName(taskInfo.get("devicename"));
	    devInfo.setCityID(Integer.parseInt(taskInfo.get("city_id")));
	    devInfo.setVendor(taskInfo.get("vendor"));
	    setDBDriver(taskInfo.get("dbdriver"));
	    setDBUrl(taskInfo.get("dburl"));
	    setDevInfo(devInfo);
	    setDevPort(Integer.parseInt(taskInfo.get("dev_port").isEmpty()?"0":taskInfo.get("dev_port")));
	    DevInfo proxdevInfo = new DevInfo();
	    proxdevInfo.setDevID(Integer.parseInt(taskInfo.get("proxy_dev_id").isEmpty()?"0":taskInfo.get("proxy_dev_id")));
	    proxdevInfo.setName(taskInfo.get("proxy_dev_name"));
	    proxdevInfo.setIP(taskInfo.get("proxy_host_ip"));
	    proxdevInfo.setHostUser(taskInfo.get("proxy_host_user"));
	    proxdevInfo.setHostPwd(taskInfo.get("proxy_host_pwd"));
	    proxdevInfo.setHostSign(taskInfo.get("proxy_host_sign"));
	    setProxyDevInfo(proxdevInfo);
	    
	    InDBServer indbserver = new InDBServer();
	    indbserver.setInDBServer(taskInfo.get("indbserver"));
	    indbserver.setInDBUser(taskInfo.get("indbuser"));
	    indbserver.setInDBPassword(taskInfo.get("indbpassword"));
	    setInDBServerConfig(indbserver);

	    setProxyDevPort(Integer.parseInt(taskInfo.get("proxy_dev_port").isEmpty()?"0":taskInfo.get("proxy_dev_port")));
	    setCollectType(Integer.parseInt(taskInfo.get("collect_type").isEmpty()?"0":taskInfo.get("collect_type")));
	    setCollectTimeOut(Integer.parseInt(taskInfo.get("collecttimeout").isEmpty()?"0":taskInfo.get("collecttimeout")));
	    setPeriod(Integer.parseInt(taskInfo.get("collect_period").isEmpty()?"0":taskInfo.get("collect_period")));
	    setCollectTime(Integer.parseInt(taskInfo.get("collect_time").isEmpty()?"0":taskInfo.get("collect_time")));
	    setCollectTimePos(Integer.parseInt(taskInfo.get("collect_timepos").isEmpty()?"0":taskInfo.get("collect_timepos")));
	    setShellCmdPrepare(taskInfo.get("shell_cmd_prepare"));
	    setShellCmdFinish(taskInfo.get("shell_cmd_finish"));

	    setParserID(Integer.parseInt(taskInfo.get("parserid").isEmpty()?"-1":taskInfo.get("parserid"))); //�������ͣ�������õĽ�����
	    setDistributorID(Integer.parseInt(taskInfo.get("distributorid").isEmpty()?"-1":taskInfo.get("distributorid")));
	
	    setRedoTimeOffset(Integer.parseInt(taskInfo.get("redo_time_offset").isEmpty()?"0":taskInfo.get("redo_time_offset")));
	    setProbeTime(Integer.parseInt(taskInfo.get("prob_starttime")));
	
	    
	    String endtime = taskInfo.get("end_data_time");
	    if(!endtime.isEmpty())
	    	this.endDataTime = new Timestamp(Util.getDate(endtime, "yyyy-MM-dd HH:mm:ss").getTime());

	    /*
	    if (Util.isOracle())
	    {
	    	String strPath = ConstDef.ClobParse(rs.getClob("COLLECT_PATH"));
	    	setCollectPath(strPath);
	    }
	    else if (Util.isSybase())
	    {
	    	setCollectPath(rs.getString("COLLECT_PATH"));
	    }
	    else if (Util.isMySQL())
	    {
	    	setCollectPath(rs.getString("COLLECT_PATH"));
	    }*/
	    setCollectPath(taskInfo.get("collect_path"));
	    
	    setShellTimeout(Integer.parseInt(taskInfo.get("shell_timeout").isEmpty()?"0":taskInfo.get("shell_timeout")));
	    setParseTmpID(Integer.parseInt(taskInfo.get("parse_tmpid").isEmpty()?"0":taskInfo.get("parse_tmpid"))); //��������
	    setParseTmpType(Integer.parseInt(taskInfo.get("tmptype_p").isEmpty()?"0":taskInfo.get("tmptype_p"))); //����ģ������

	    this.parseTmpRecord = new TempletRecord();
	    this.parseTmpRecord.setId(Integer.parseInt(taskInfo.get("parse_tmpid").isEmpty()?"0":taskInfo.get("parse_tmpid"))); //����ģ����
	    this.parseTmpRecord.setType(Integer.parseInt(taskInfo.get("tmptype_p").isEmpty()?"0":taskInfo.get("tmptype_p"))); //����ģ������
	    this.parseTmpRecord.setName(taskInfo.get("tmpname_p")); 
	    this.parseTmpRecord.setEdition(taskInfo.get("edition_p"));
	    this.parseTmpRecord.setFileName(taskInfo.get("tempfilename_p"));

	    //����ģ��
	    this.parseTemplet = Factory.createTemplet(this.parseTmpRecord);
	
	    setDisTmpID(Integer.parseInt(taskInfo.get("distrbute_tmpid").isEmpty()?"0":taskInfo.get("distrbute_tmpid")));
	
	    this.distTmpRecord = new TempletRecord();
	    this.distTmpRecord.setId(Integer.parseInt(taskInfo.get("distrbute_tmpid").isEmpty()?"0":taskInfo.get("distrbute_tmpid")));
	    this.distTmpRecord.setType(Integer.parseInt(taskInfo.get("tmptype_d").isEmpty()?"0":taskInfo.get("tmptype_d")));
	    this.distTmpRecord.setName(taskInfo.get("tmpname_d"));
	    this.distTmpRecord.setEdition(taskInfo.get("edition_d"));
	    this.distTmpRecord.setFileName(taskInfo.get("tempfilename_d"));

	    this.distributeTemplet = Factory.createTemplet(this.distTmpRecord);
	
	    String suctime = taskInfo.get("suc_data_time");
	    
	    setLastCollectTime(new Timestamp(Util.getDate(suctime, "yyyy-MM-dd HH:mm:ss").getTime()));
	
	    setLastCollectPos(Integer.parseInt(taskInfo.get("suc_data_pos").isEmpty()?"0":taskInfo.get("suc_data_pos")));
	
	    setMaxReCollectTime(Integer.parseInt(taskInfo.get("maxclttime").isEmpty()?"0":taskInfo.get("maxclttime")));
	
	    setThreadSleepTime(Integer.parseInt(taskInfo.get("threadsleeptime").isEmpty()?"0":taskInfo.get("threadsleeptime")));
	
	    setBlockedTime(Integer.parseInt(taskInfo.get("blockedtime").isEmpty()?"0":taskInfo.get("blockedtime")));
	    
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
  		}
  		if (bAdd)
  		{
  			startTask();
	  	}
  	}

  	public void startTask()
  	{
  		if (TaskMgr.getInstance().addTask(this.getTaskID(),this.getLastCollectTime().getTime()))
  		{
  			AbstractAccessor accessor = Factory.createAccessor(this);
  			setCollectThread(accessor);
  			//accessor.s.setName("Thread:"+this.getDescribe() + "[" + this.getLastCollectTime() + "]");
  			//accessor.start();
  			accessor.setBeginExceuteTime(new Date());
  			this.startTime = new Timestamp(new Date().getTime());
  			accessor.setSubmitTime(new Date());
  			
  			ThreadPool.getInstance().addTask(accessor);
  			
  			//������һ���ɼ�ʱ�� 2013-06-03 Turk
  			long lastCollectTime = getLastCollectTime().getTime();
  			Timestamp timeStamp = new Timestamp(lastCollectTime + getPeriodTime());
  	  		saveLastCollectTime(timeStamp);
  		}
  	}

  	/**
  	 * �ж��Ƿ�����вɼ����˴����Ʋɼ���ʱ�Ͳɼ�����ʱ���
  	 * @param unit
  	 * @param scanTime
  	 * @return
  	 */
  	protected boolean isReady(int unit, long scanTime)
  	{
  		boolean bReturn = false;

  		//�ɼ���������ʱ�䣬��ɼ�ΪСʱ��Сʱ�ɼ�Ϊ����
  		int collectTime = getCollectTime();
  		collectTime = collectTime > 59 ? 0 : collectTime;
  		
  		//�ɼ�����ƫ�ƶ��ٷ��ӿ�ʼ�ɼ�
  		long startTime = getLastCollectTime().getTime() + 
  			getCollectTimePos() * 60 * 1000;

  		if (scanTime - startTime >= getPeriodTime())
  		{
  			DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(getTaskID()));
  			bReturn = true;
  		}
  		else if ((unit >= collectTime) && (startTime < scanTime))
  		{
  			DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(getTaskID()));
  			bReturn = true;
  		}

  		//if (!bReturn)
  		//{
  		//	TaskMgr.getInstance().tempTasks.add(this);
  		//}
  		return bReturn;
  	}

  	
  	/**
  	 * ���ݲɼ����ڿ��Ʋɼ�ʱ���
  	 * @return
  	 */
  	public long getPeriodTime()
  	{
  		long time = 0L;
  		switch (getPeriod())
  		{
  			case 1://
  				time = 60000L;
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
  			case 5://15 min
  				time = 900000L;
  				break;
  			case 7://5 min
  				time = 300000L;
  				break;
  			case 8:
  				time = 43200000L;
  				break;
  			case 9:
  				time = 60000L;
  				break;
  			case 10://ʮ��������
  				time = 10*60*1000L;
  		}

  		return time;
  	}

  	/**
  	 * �ɼ��������
  	 * @return
  	 */
  	public boolean doAfterCollect()
  	{
  		TaskMgr.getInstance().removeTask(this.getTaskID(),this.getLastCollectTime().getTime());
  		
  		
  		if(getPeriod()==1)
  			return true;//ʵʱ�ɼ�
  		
  		
  		//֪ͨ���ڵ��������
  		long lastCollectTime = getLastCollectTime().getTime();
  		//�ɼ���ɺ󣬼�¼��Ϣ����������
  		return setResultTimePos(getTaskID(), new Timestamp(lastCollectTime), 0);
  		
  		//Timestamp timeStamp = new Timestamp(lastCollectTime + getPeriodTime());
  		//return saveLastCollectTime(timeStamp);
  	}

  	/**
  	 * �ɼ���ɺ󱣴����²ɼ�ʱ���
  	 * @param time
  	 * @return
  	 */
  	public boolean saveLastCollectTime(Timestamp time)
  	{
  		if(this.isPersistentTask)//����ǳ����Բɼ����񣬲���Ҫ���²ɼ���־
  			return true;
  		
  		
  		//������һ��������ʱ�䣬��ʾ�����������������������
  		if(SystemConfig.getInstance().IsTaskUserXML())
  		{
  			TaskMgr.getInstance().setLastImportTimePosForXML(getTaskID(), time, 0);
  		}
  		else
  		{
  			TaskMgr.getInstance().setLastImportTimePos(getTaskID(), time, 0);
  		}
  		
  		String logStr = this.sysName + ": update stamptime :" + getDescribe() + "  " + time;
  		this.log.debug(logStr);
  		log("����", logStr);
  		return true;
  	}
  	
  	/**
  	 * ��¼�ɼ������Ϣ
  	 * @param taskID
  	 * @param ts
  	 * @param pos
  	 */
  	protected boolean setResultTimePos(int taskID, Timestamp ts, int pos)
  	{
  		log.debug("Turk-setResultTimePos");
  		String strTime = Util.getDateString(ts);
  		
  		long lastCollectTime = getLastCollectTime().getTime();
		Timestamp timeStamp = new Timestamp(lastCollectTime + getPeriodTime());
			
  		setLastCollectTime(timeStamp);
  		setLastCollectPos(0);
  		
  		
  		
  		StringBuffer sb = new StringBuffer();
  		if (Util.isOracle())
  		{
  			sb.append("insert into utl_conf_task_result (task_id,suc_data_time,result_time)");
  			sb.append("values (" + taskID + ",to_date('" + 
  					strTime + "','YYYY-MM-DD HH24:MI:SS'),sysdate)");
  			log.debug("д�����������־�����ݿ�� utl_conf_task_result");
  		}
  		else if (Util.isSybase())
  		{
  			sb.append("insert into utl_conf_task_result (task_id,suc_data_time,result_time)");
  			sb.append("values (" + taskID + ",convert(datetime,'" + 
  					strTime + "'),sysdate())");
  		
  		}
  		else if (Util.isMySQL())
  		{
  			sb.append("insert into utl_conf_task_result (task_id,suc_data_time,result_time)");
  			sb.append("values (" + taskID + ",'" + 
  					strTime + "',sysdate())");
  		}

  		try
  		{
  			CommonDB.executeUpdate(sb.toString());
  		}
  		catch (SQLException e)
  		{
  			log.error("Task-" + taskID + ": ���������ʱ�䡢λ��ʱ����.ԭ��:", e);
  			return false;
  		}
  		return true;
  	}

  	public Timestamp getEndDataTime()
  	{
  		return this.endDataTime;
  	}

  	public void setEndDataTime(Timestamp endDataTime)
  	{
  		this.endDataTime = endDataTime;
  	}

  	public String getTempTempFileName()
  	{
  		return this.tempTempFileName;
  	}

  	public void setTempTempFileName(String tempTempFileName)
  	{
  		this.tempTempFileName = tempTempFileName;
  	}

  	public String toString()
  	{
  		return this.sysName;
  	}

  	/**
  	 * �������ͣ���Ӧ���ǲɼ����������еĽ������ͣ����ڷ��������
  	 * @return
  	 */
  	public int getParserID()
  	{
  		return this.parserID;
  	}

  	/**
  	 * �������ͣ���Ӧ���ǲɼ����������еĽ������ͣ����ڷ��������
  	 * @param parserID
  	 */
  	public void setParserID(int parserID)
  	{
  		this.parserID = parserID;
  	}

  	public int getDistributorID()
  	{
  		return this.distributorID;
  	}

  	public void setDistributorID(int distributorID)
  	{
  		this.distributorID = distributorID;
  	}

  	public int getRedoTimeOffset()
  	{
  		return this.redoTimeOffset;
  	}

  	public void setRedoTimeOffset(int redoTimeOffset)
  	{
  		this.redoTimeOffset = redoTimeOffset;
  	}

  	public int getDisTmpID()
  	{
  		return this.disTmpID;
  	}

  	public void setDisTmpID(int disTmpID)
  	{
  		this.disTmpID = disTmpID;
  	}

  	public String getSysName()
  	{
  		return this.sysName;
  	}	

  	public int getProbeTime()
  	{
  		return this.probeTime;
  	}

  	public void setProbeTime(int probeTime)
  	{
  		this.probeTime = probeTime;
  	}
  	
  	/**
  	 * �Ƿ�Ϊ�����Բɼ�����
  	 * @param ispersistenttask
  	 */
  	public void setPersistentTask(boolean ispersistenttask)
  	{
  		isPersistentTask = ispersistenttask;
  	}
  	
  	/**
  	 * �Ƿ�Ϊ�����Բɼ�����
  	 * @return
  	 */
  	public boolean getPersistentTask()
  	{
  		return isPersistentTask;
  	}

  	@Deprecated
  	public DataLogInfo getLogInfo()
  	{
  		return this.logInfo;
  	}

  	public void log(String taskStatus, String taskDetail, Throwable taskException, String taskResult)
  	{
  		this.logInfo.setTaskId(getTaskID());
	    this.logInfo.setTaskDescription(getDescribe());
	    this.logInfo.setTaskType((this instanceof RegatherObjInfo) ? "��������" : "��������");
	    this.logInfo.setTaskStatus(taskStatus);
	    this.logInfo.setTaskDetail(taskDetail);
	    this.logInfo.setTaskException(taskException);
	    this.logInfo.setDataTime(getLastCollectTime());
	    this.logInfo.setCostTime(this.startTime == null ? 0L : new Date().getTime() - 
	      this.startTime.getTime());
	    this.logInfo.setTaskResult(taskResult);
	    this.logInfo.addLog();
  	}

  	public void log(String taskStatus, String taskDetail)
  	{
  		log(taskStatus, taskDetail, null);
  	}

  	public void log(String taskStatus, String taskDetail, Throwable taskException)
  	{
  		log(taskStatus, taskDetail, taskException, null);
  	}
  	
}