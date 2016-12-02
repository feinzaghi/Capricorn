package com.turk.task;

import com.turk.Config.ConstDef;
import com.turk.Config.SystemConfig;

import java.io.File;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import oracle.sql.CLOB;

import org.apache.log4j.Logger;

import com.turk.alarm.AlarmMgr;
import com.turk.delayprobe.DelayProbeMgr;
import com.turk.task.Config.taskInfo;

import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.Task;
import com.turk.util.ThreadPool;
import com.turk.util.Util;

/**
 * 任务管理
 * @author Administrator
 *
 */
public class TaskMgr
{
	/**
	 * 活动任务
	 */
	private Map<Integer, CollectObjInfo> activeTasks;
	
	private ArrayList<String> activeList = new ArrayList<String>();
	
	/**
	 * 活动补采任务
	 */
	private Map<Integer, RegatherObjInfo> activeTasksForRegather;
	
	/**
	 * 标志位检查
	 */
	private boolean checkFlag = true;
	
	private Map<Integer, HashMap<Long, RegatherStruct>> regatherMap;
	
	/**
	 * 
	 */
	public List<CollectObjInfo> tempTasks = new ArrayList<CollectObjInfo>();

	private Logger applog = LogMgr.getInstance().getAppLogger("taurus");
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Logger errorlog = LogMgr.getInstance().getErrorLogger();

	private TaskMgr()
	{
		this.activeTasks = new HashMap();//活动采集任务
		this.activeTasksForRegather = new HashMap();//活动补采任务
		this.regatherMap = new HashMap();

		this.checkFlag = checkSelf();
	}

	public static TaskMgr getInstance()
	{
		return TaskMgrContainer.instance;
	}

	/**
	 * 采集任务列表
	 * @return
	 */
	public List<CollectObjInfo> list()
	{
		List lst = new ArrayList();

		Collection<CollectObjInfo> cObjs = this.activeTasks.values();
		for (CollectObjInfo obj : cObjs)
		{
			lst.add(obj);
		}

		Collection<RegatherObjInfo> cRObjs = this.activeTasksForRegather.values();
		for (RegatherObjInfo obj : cRObjs)
		{
			lst.add(obj);
		}

		return lst;
	}
	
	/**
  	 * 判断是否为重采任务
  	 * @param taskInfo
  	 * @return
  	 */
  	public boolean isReAdoptObj(CollectObjInfo taskInfo)
  	{
  		boolean flag = false;
  		if ((taskInfo != null) && ((taskInfo instanceof RegatherObjInfo)))
  		{
  			flag = true;
  		}
  		return flag;
  	}
  
  	/**
  	 * 通过采集任务获取数据库连接
  	 * @param task
  	 * @param iSleepTime
  	 * @param maxTryTimes
  	 * @return
  	 */
  	public Connection getConnection(CollectObjInfo task, int iSleepTime, byte maxTryTimes)
  	{
  		if (task == null) {
  			return null;
  		}
  		Connection conn = null;

  		conn = CommonDB.getConnection(task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

  		if (conn == null)
  		{
  			String strLog = "Task-" + task.getTaskID();

  			log.error(strLog + ": 获取对方数据库连接失败,尝试重连 ... ");

  			byte tryTimes = 0;
  			int sleepTime = iSleepTime;
  			while ((tryTimes < maxTryTimes) && (conn == null))
  			{
  				try
  				{
  					Thread.sleep(sleepTime);
  				}
  				catch (InterruptedException e)
  				{
  					break;
  				}

  				conn = CommonDB.getConnection(task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

  				tryTimes = (byte)(tryTimes + 1);

  				if (conn == null)
  				{
  					errorlog.error(strLog + ": 尝试数据库重连失败 (" + tryTimes + ") ... ");
  				}

  				sleepTime += sleepTime * 2;
  			}

  			if (conn == null)
  			{
  				errorlog.error(strLog + ": 多次获取对方数据库连接失败.");
  			}
  			else
  			{
  				errorlog.debug(strLog + ": 数据库重连成功(" + tryTimes + ").");
  			}
  		}

  		return conn;
  	}

	private boolean checkSelf()
	{
		String localHostName = Util.getHostName();
		boolean b = true;
		if (Util.isNull(localHostName)) {
			b = false;
		}
		return b;
	}
	
	/**
	 * 添加任务
	 * @param obj
	 * @return
	 */
	public synchronized boolean addTask(int taskid,long timestamp)
	{
		String key = taskid + "_" + timestamp;
		if(activeList.contains(key))
		{
			return false;
		}
		else
		{
			activeList.add(key);
			return true;
		}
	}
	
	public synchronized void removeTask(int taskid,long timestamp)
	{
		String key = taskid + "_" + timestamp;
		if(activeList.contains(key))
		{
			activeList.remove(key);
		}
	}
	
	

	/**
	 * 添加任务
	 * @param obj
	 * @return
	 */
	public synchronized boolean addTask(CollectObjInfo obj)
	{
		if (obj == null) {
			return false;
		}

		//检查是否为补采任务
		boolean isReclt = obj instanceof RegatherObjInfo;

		int keyID = obj.getKeyID();
		if ((!isActive(keyID, isReclt)) /*&& (!isMaxThreadCount(isReclt))*/)
		{
			if (isReclt)
			{//不让补采线程占系统大部分资源，每次只放入指定量的补采任务
				if(!isMaxThreadCount(isReclt))
				{
					this.activeTasksForRegather.put(Integer.valueOf(keyID), (RegatherObjInfo)obj);
				}
				else
				{
					return false;
				}
			}
			else
			{
				this.activeTasks.put(Integer.valueOf(keyID), obj);
			}
			return true;
		}

		return false;
	}

	/**
	 * 根据任务号检查该任务是否为活动任务
	 * @param taskID 任务编号
	 * @param isReclt 是否为补采任务
	 * @return
	 */
	public synchronized boolean isActive(int taskID, boolean isReclt)
	{
		Map map = isReclt ? this.activeTasksForRegather : this.activeTasks;
		boolean bExist = map.containsKey(Integer.valueOf(taskID));

		/* 将原来任务对象修改为线程池任务对象后，此处代码不需要了  2013/06/04 Turk
		if (bExist)
		{
			CollectObjInfo cltobj = (CollectObjInfo)map.get(Integer.valueOf(taskID));

			int iBlockedTime = cltobj.getBlockedTime();
			long currTime = System.currentTimeMillis();

			if ((iBlockedTime != 0) && 
					((currTime - cltobj.startTime.getTime()) / 1000L / 60L >= iBlockedTime))
			{
				Task hClt = cltobj.getCollectThread();
				//hClt.interrupt();
				hClt = null;
				bExist = false;
				delActiveTask(taskID, isReclt);
				log.warn("任务-" + taskID + "[" + 
						Util.getDateString(cltobj.getLastCollectTime()) + 
						"]:运行时间已经超过" + iBlockedTime + "分钟，被强制终止");
				AlarmMgr.getInstance().insert(taskID,(byte)2, "任务超时", cltobj.getHostName(), "任务-" + taskID + "[" + 
						Util.getDateString(cltobj.getLastCollectTime()) + 
						"]:运行时间已经超过" + iBlockedTime + "分钟，被强制终止", 40101);
			}
		}*/

		return bExist;
	}

	/**
	 * 判断是否为活动线程
	 * @param keyID
	 * @param taskID
	 * @param filePath
	 * @param ts
	 * @param isReclt
	 * @return
	 */
	public synchronized boolean isActive(int keyID, int taskID, String filePath, Timestamp ts, boolean isReclt)
	{
		Map map = isReclt ? this.activeTasksForRegather : this.activeTasks;

		boolean bExist = map.containsKey(Integer.valueOf(keyID));
		/* 将原来任务对象修改为线程池任务对象后，此处代码不需要了  2013/06/04 Turk
		long currTime;
		if (bExist)
		{
			CollectObjInfo cltobj = (CollectObjInfo)map.get(Integer.valueOf(keyID));

			int iBlockedTime = cltobj.getBlockedTime();
			currTime = System.currentTimeMillis();

			if ((iBlockedTime != 0) && 
					((currTime - cltobj.startTime.getTime()) / 1000L / 60L >= iBlockedTime))
			{
				Task hClt = cltobj.getCollectThread();
		        //hClt.interrupt();
		        hClt = null;
		        bExist = false;
		        delActiveTask(keyID, isReclt);
		        log.warn("任务-" + taskID + "[" + 
		          Util.getDateString(cltobj.getLastCollectTime()) + 
		          "]:运行时间已经超过" + iBlockedTime + "分钟，被强制终止");
		        
		        AlarmMgr.getInstance().insert(taskID,(byte)2, "任务超时", cltobj.getHostName(), "任务-" + taskID + "[" + 
						Util.getDateString(cltobj.getLastCollectTime()) + 
						"]:运行时间已经超过" + iBlockedTime + "分钟，被强制终止", 40101);
			}
		}
		else
		{
			Collection<CollectObjInfo> values = map.values();
			for (CollectObjInfo obj : values)
			{
				int tTaskID = obj.getTaskID();
		        String tCollectPath = obj.getCollectPath();
		        Timestamp tTS = obj.getLastCollectTime();
		        if ((taskID != tTaskID) || (ts.getTime() != tTS.getTime()))
		        	continue;
		        if (!tCollectPath.contains(filePath))
		        	continue;
		        log.warn("Task-" + taskID + "-" + keyID + 
		        		": 业务意义上出现重复的任务. 数据源=" + filePath + " 数据时间=" + 
		        		Util.getDateString(tTS));
		        return true;
			}

		}*/

		return bExist;
	}

	/**
	 * 删除活动线程
	 * @param taskID
	 * @param isReclt
	 */
	public synchronized void delActiveTask(int taskID, boolean isReclt)
	{
		Map map = isReclt ? this.activeTasksForRegather : this.activeTasks;
		if (map.containsKey(Integer.valueOf(taskID)))
			map.remove(Integer.valueOf(taskID));
	}

	/**
	 * 判断是否达到最大线程数
	 * @param isReclt
	 * @return
	 */
	public synchronized boolean isMaxThreadCount(boolean isReclt)
	{
		Map map = isReclt ? this.activeTasksForRegather : this.activeTasks;

		int size = map.size();
		SystemConfig sc = SystemConfig.getInstance();
		int maxThreadCount = isReclt ? sc.getMaxRecltCount() : sc.getMaxCltCount();

		if ((size < maxThreadCount) || (maxThreadCount <= 0)) {
			return false;
		}

		log.warn("[" + (isReclt ? "补采任务" : "正常任务") + 
				"]负荷过大,原因:已达到本机最大运行线程数(" + maxThreadCount + "),任务需等待！");
		return true;
	}

	public Map<Integer, CollectObjInfo> getTasksMap()
	{
		return this.activeTasks;
	}

	public Map<Integer, RegatherObjInfo> getActiveTasksForRegather()
	{
		return this.activeTasksForRegather;
	}

	public synchronized CollectObjInfo getTask(int taskID)
	{
		return (CollectObjInfo)this.activeTasks.get(Integer.valueOf(taskID));
	}

	/**
	 * 当前活动线程数
	 * @return
	 */
	public synchronized int size()
	{
		return ThreadPool.getInstance().ActiveTaskCount() + ThreadPool.getInstance().getThreadQueueCount();
	}

  
  	/**
  	 * 加载正常采集任务
  	 * @param scanDate 扫描时间
  	 * @return
  	 */
  	public boolean loadNormalTasksFromDB(Date scanDate)
  	{
  		boolean bReturn = false;
  		if (!this.checkFlag) {
  			return bReturn;
  		}
  		log.info("开始加载任务信息...");

  		if(SystemConfig.getInstance().IsTaskUserXML())
  		{
  			bReturn = getCollectInfoByXML(scanDate);
  		}
  		else
  		{
  			bReturn = getCollectInfo(scanDate);
  		}

  		log.debug("load tasks from DB. --Done(" + bReturn + ")");

  		return bReturn;
  	}
  	
  	/**
  	 * 加载持续性采集任务
  	 * @param scanDate 扫描时间
  	 * @return
  	 */
  	public boolean loadPersistentTasksFromDB(Date scanDate)
  	{
  		if (!this.checkFlag) {
  			return false;
  		}
  		log.info("开始加载任务信息...");

  		boolean bReturn = getPersistentCollectInfo(scanDate);

  		log.debug("load Persistent tasks from DB. --Done(" + bReturn + ")");

  		return bReturn;
  	}

  	/**
  	 * 读取补采信息表
  	 */
  	public void loadReGatherTasksFromDB()
  	{
  		if (!this.checkFlag) {
  			return;
  		}
  		log.info("开始加载补采表任务信息...");

  		boolean bReturn = getRegatherInfo();

  		log.debug("load r-tasks from DB. --Done(" + bReturn + ")");
  	}

  	public void setLastImportTimePos(int taskID, Timestamp ts, int pos)
  	{
  		String strTime = Util.getDateString(ts);
  		StringBuffer sb = new StringBuffer();
  		if (Util.isOracle())
  		{
  			sb.append("update UTL_CONF_TASK set suc_data_time=to_date('" + 
  					strTime + "','YYYY-MM-DD HH24:MI:SS'),suc_data_pos=" + 
  					pos + " where TASK_ID =" + taskID);
  		}
  		else if (Util.isSybase())
  		{
  			sb.append("update UTL_CONF_TASK set suc_data_time=convert(datetime,'" + 
  					strTime + 
  					"'),suc_data_pos=" + 
  					pos + 
  					" where TASK_ID =" + 
  					taskID);
  		}
  		else if (Util.isMySQL())
  		{
  			sb.append("update UTL_CONF_TASK set suc_data_time='" + 
  					strTime + "',suc_data_pos=" + pos + " where TASK_ID =" + taskID);
  		}

  		try
  		{
  			CommonDB.executeUpdate(sb.toString());
  		}
  		catch (SQLException e)
  		{
  			log.error("Task-" + taskID + ": 更新最后导入时间、位置时出错.原因:", e);
  		}
  	}
  	
  	/**
  	 * 从XML文件读取任务
  	 * @param taskID
  	 * @param ts
  	 * @param pos
  	 */
  	public void setLastImportTimePosForXML(int taskID, Timestamp ts, int pos)
  	{
  		try {
  			taskInfo task = new taskInfo();
  			File taskfile = new File(SystemConfig.getInstance().getTaskConfigPath()
  				+ File.separator + "Task_" + taskID + ".xml");
		
  			Date time = new Date(ts.getTime());
  			String suc_data_time = Util.getDateString_Standard_ss(time);
			
			task.SetTaskInfo(taskfile.getAbsolutePath(), "suc_data_time", suc_data_time);
			task.SetTaskInfo(taskfile.getAbsolutePath(), "suc_data_pos", String.valueOf(pos));
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
  	}

  	/**
  	 * 加入新补采任务
  	 * @param taskInfo
  	 * @param filePath
  	 * @param cause
  	 */
  	public synchronized void newRegather(CollectObjInfo taskInfo, String filePath, String cause)
  	{
  		log.warn(cause + "  文件路径:" + filePath);
  		
  		if ((taskInfo == null) || (filePath == null)) {
  			return;
  		}
  		
  		if(taskInfo.getParserID() == 8)
  		{//Taurus 解析 需要告警文件
  			Calendar cal = Calendar.getInstance();
  			cal.setTime(taskInfo.getLastCollectTime());
//  			if(filePath.toUpperCase().contains("A1CC") || filePath.length() == 0)
//  			{
//  				SimpleDateFormat f = new SimpleDateFormat("MM月dd日 HH:mm");
//  				String strTime = f.format(taskInfo.getLastCollectTime());
//  				//请注意：MCS系统XX月XX日 XX:XX 联通GSM（或者电信、移动GSM）语音文件大小为0.
//  				String smsContent = String.format("请注意：MCS系统 %s %s 语音文件:%s",
//  						strTime,taskInfo.getDescribe(),cause);
//  				SendSMS sms = new SendSMS(SystemConfig.getInstance().TaurusSMS(),
//  						smsContent);
//  				sms.start();
//  				applog.debug("Send SMS:" + smsContent);
//  			}
  		}

  		if (taskInfo.getMaxReCollectTime() == -1) {
  			//无需要补采
  			return;
  		}
  		int keyID = taskInfo.getKeyID();
  		int taskID = taskInfo.getTaskID();
  		Timestamp dataTime = taskInfo.getLastCollectTime();

  		//判读当前补采任务是否存在
  		if (this.regatherMap.containsKey(Integer.valueOf(keyID)))
  		{
  			HashMap<Long, RegatherStruct> map = (HashMap)this.regatherMap.get(Integer.valueOf(keyID));
  			if (map == null)
  			{
  				this.regatherMap.remove(Integer.valueOf(keyID));
  				return;
  			}

  			long time = dataTime.getTime();
  			if (map.containsKey(Long.valueOf(time)))
  			{
  				RegatherStruct struct = (RegatherStruct)map.get(Long.valueOf(time));
  				if (struct == null)
  				{
  					map.remove(Long.valueOf(time));
  					return;
  				}

  				struct.addDS(filePath);
  			}
  			else
  			{
  				List<String> ds = new ArrayList<String>();
  				ds.add(filePath);
  				RegatherStruct struct = new RegatherStruct(keyID, taskID, ds, dataTime, cause);

  				map.put(Long.valueOf(time), struct);
  			}
  		}
  		else
  		{
  			List<String> ds = new ArrayList<String>();
  			ds.add(filePath);

  			RegatherStruct struct = new RegatherStruct(keyID, taskID, ds, dataTime, cause);
  			HashMap<Long, RegatherStruct> map = new HashMap<Long, RegatherStruct>();
  			map.put(Long.valueOf(dataTime.getTime()), struct);

  			this.regatherMap.put(Integer.valueOf(keyID), map);
  		}
  	}

  	/**
  	 * 加入新补采任务
  	 * @param taskInfo
  	 * @param filePath
  	 */
  	public synchronized void newRegather(CollectObjInfo taskInfo, String filePath)
  	{
  		newRegather(taskInfo, filePath, "");
  	}

  	/**
  	 * 提交补采任务入库
  	 * @param obj
  	 * @param time
  	 */
  	public synchronized void commitRegather(CollectObjInfo obj, long time)
  	{
  		if (obj == null) {
  			return;
  		}

  		if (obj.getMaxReCollectTime() == -1) {
  			return;
  		}
  		int keyID = obj.getKeyID();
  		int taskID = obj.getTaskID();

  		if (!this.regatherMap.containsKey(Integer.valueOf(keyID))) {
  			return;
  		}
  		HashMap<?, ?> map = (HashMap<?, ?>)this.regatherMap.get(Integer.valueOf(keyID));
  		if ((map == null) || (!map.containsKey(Long.valueOf(time)))) {
  			return;
  		}
  		RegatherStruct struct = (RegatherStruct)map.get(Long.valueOf(time));
  		if (struct == null) {
  			return;
  		}
  		if (struct.taskID != taskID) {
  			return;
  		}

  		boolean bOK = putRegatherToDB(struct);
  		if (bOK)
  		{
  			map.remove(Long.valueOf(time));
  			if (map.size() == 0)
  				this.regatherMap.remove(Integer.valueOf(keyID));
  		}
  	}

  	/**
  	 * 将补采任务写入数据库补采任务表
  	 * @param struct
  	 * @return
  	 */
  	private boolean putRegatherToDB(RegatherStruct struct)
  	{
  		String localHostName = Util.getHostName();
  		boolean bReturn = true;

	    int taskID = struct.getTaskID();
	    int keyID = struct.getKey();
	    List<String> ds = struct.getDs();
	    String cause = struct.getCause();
	    if ((ds == null) || (ds.size() == 0)) {
	    	return false;
	    }
	    int id = keyID - 10000000;
	    String collectTime = Util.getDateString(struct.getDataTime());

	    PreparedStatement pstmt = null;
	    Connection conn = null;
	    StringBuffer sb = null;
	    try
	    {
	    	conn = CommonDB.getConnection();
	    	if (conn == null)
	    	{
	    		log.error("Task-" + taskID + "-" + keyID + 
	    		": 添加补采任务失败,原因:无法获取数据库连接.");
	    		return false;
	    	}

	    	/*if (SystemConfig.getInstance().getMRProcessId() != 0) {
	    		localHostName = localHostName + "@" + 
	    		SystemConfig.getInstance().getMRProcessId();
	    	}*/
	    	for (String filePath : ds)
	    	{
	    		if (filePath == null)
	    			continue;
	    		sb = new StringBuffer();
	    		String stampTime = Util.getDateString(new Date());
	    		if (Util.isOracle())
	    		{
	    			String sql = "select (ID + 10000000) as key,FILEPATH from UTL_CONF_RTASK where TASKID=" + 
	    			taskID + 
			            " and collectstatus=0 and id<>" + 
			            id + 
			            " and COLLECTTIME=to_date('" + 
			            collectTime + 
			            "','YYYY-MM-DD HH24:MI:SS')";
	    			pstmt = conn.prepareStatement(sql);
	    			ResultSet rs = pstmt.executeQuery();
	    			List<String> temp = new ArrayList();
	    			while (rs.next())
	    			{
	    				if (rs.getClob("FILEPATH") != null)
	    				{
	    					temp.add(ConstDef.ClobParse(rs.getClob("FILEPATH")));
	    				}
	    				else
	    				{
	    					temp.add("");
	    				}
	    			}
	    			boolean bExist = false;
	    			if (temp.size() > 0)
	    			{
	    				for (String s : temp)
	    				{
	    					if (!s.contains(filePath))
	    						continue;
	    					bExist = !Util.isNull(filePath);
	    					break;
	    				}

	    			}

	    			if (!bExist)
	    			{
	    				Connection con = DbPool.getConn();
	    				Statement st = null;
	    				ResultSet res = null;
	    				try
	    				{
	    					con.setAutoCommit(false);
	    					st = con.createStatement();
	    					String insertsql = " insert into UTL_CONF_RTASK(ID,TASKID,COLLECTOR_NAME,FILEPATH,COLLECTTIME,STAMPTIME,COLLECTSTATUS,READOPTTYPE,CAUSE) values (SEQ_UTL_CONF_RTASK.Nextval," + 
					                taskID + 
					                ",'" + 
					                localHostName + 
					                "'," + 
					                "empty_clob()" + 
					                ",to_date('" + 
					                collectTime + 
					                "','YYYY-MM-DD HH24:MI:SS'),to_date('" + 
					                stampTime + 
					                "','YYYY-MM-DD HH24:MI:SS'),0,0,empty_clob())";
	    					st.execute(insertsql);
	    					int seq = 0;
	    					res = st.executeQuery("select SEQ_UTL_CONF_RTASK.currval from dual");
	    					res.next();
	    					seq = res.getInt(1);

	    					String selectsql = "select FILEPATH,CAUSE from UTL_CONF_RTASK where id=" + 
	    						seq + " for update";
	    					res = st.executeQuery(selectsql);
	    					res.next();
	    					CLOB clob = (CLOB)res.getClob("FILEPATH");
	    					Writer out = clob.getCharacterOutputStream();
	    					out.write(filePath);
	    					out.flush();
	    					out.close();
	    					clob = (CLOB)res.getClob("CAUSE");
	    					out = clob.getCharacterOutputStream();
	    					out.write(cause);
	    					out.flush();
	    					out.close();
	    					if (con != null)
	    					{
	    						con.commit();
	    					}
	    					log.debug("id:" + seq + ",taskid:" + taskID + 
	    						"  被加入UTL_CONF_RTASK表");
	    				}
	    				catch (Exception e)
	    				{
	    					log.error("加入补采时异常，taskid:" + taskID + 
	    							"  filepath:" + filePath, e);
	    					if (con != null)
	    					{
	    						con.rollback();
	    					}
	    				}
	    				finally
	    				{
	    					if (res != null)
	    					{
	    						res.close();
	    					}
	    					if (st != null)
	    					{
	    						st.close();
	    					}
	    					if (con != null)
	    					{
	    						con.close();
	    					}
	    				}
	    			}
	    		}
	    		else if (Util.isSybase())
	    		{
	    			sb.append(" insert into UTL_CONF_RTASK(TASKID,COLLECTOR_NAME,FILEPATH,COLLECTTIME) values (" + 
	    					taskID + 
					            ",'" + 
					            localHostName + 
					            "'," + 
					            "'" + 
					            filePath + 
					            "',convert(datetime,'" + 
					            collectTime + 
					            "'))");
	    		}

	    		if ((sb == null) || (!Util.isNotNull(sb.toString())))
	    			continue;
	    		log.debug("Task-" + taskID + "-" + keyID + 
	    				": 准备执行插入补采SQL语句= " + sb.toString());

	    		pstmt = conn.prepareStatement(sb.toString());
	    		pstmt.executeUpdate();

	    		log.debug("Task-" + taskID + "-" + keyID + 
	    				" 被加入补采表中. (数据时间=" + collectTime + ",数据源=" + 
	    				filePath + ")");
	    	}

	    }
	    catch (Exception e)
	    {
	    	errorlog.error("Task-" + taskID + "-" + keyID + ": 添加补采任务时异常sql=" + 
	    			sb.toString() + " (数据时间=" + collectTime + "),原因:", e);
	    	bReturn = false;
	    	try
	    	{
	    		if (pstmt != null)
	    			pstmt.close();
	    		if (conn != null)
	    			conn.close();
	    	}
	    	catch (Exception localException2)
	    	{
	    	}
	    }
	    finally
	    {
	    	try
	    	{
	    		if (pstmt != null)
	    			pstmt.close();
	    		if (conn != null) {
	    			conn.close();
	    		}
	    	}
	    	catch (Exception localException3)
	    	{
	    	}
	    }
	    return bReturn;
  	}
  	
  	/**
  	 * 使用XML配置的任务对象
  	 * @param scandate
  	 * @return
  	 */
  	private boolean getCollectInfoByXML(Date scandate)
  	{
  		String localHostName = Util.getHostName();
  		boolean bReturn = false;
  		//扫描配置目录下的任务文件
  		CollectObjInfo info;
  		
  		File taskfiles = new File(SystemConfig.getInstance().getTaskConfigPath());
  		for(File taskfile : taskfiles.listFiles())
  		{
  			try{
				//构建任务
  				if(!taskfile.getName().contains("Task"))
  					continue;
  				taskInfo task = new taskInfo();
  				task.parseTask(taskfile.getAbsolutePath());
  				HashMap<String,String> taskmap = task.TaskInfo;
  				
  				int taskID = Integer.parseInt(taskmap.get("task_id"));
  				
				info = new CollectObjInfo(taskID);
				info.buildObj(taskmap,scandate);
				info.setHostName(localHostName);				
				this.tempTasks.add(info);
				
		  		
			}catch (Exception e){
				errorlog.error("构建任务时异常.原因:", e);
			}
  		}
  		
  		bReturn = true;
  		return bReturn;
  	}

  	private boolean getCollectInfo(Date scandate)
  	{
  		String localHostName = Util.getHostName();
  		boolean bReturn = false;

  		PreparedStatement pstmt = null;
  		ResultSet rs = null;

  		Connection conn = null;
  		try
  		{
  			log.debug("Starting getConnection...");
  			conn = CommonDB.getConnection();
  			log.debug("GetConnection done...");
  			if (conn == null)
  			{
  				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
  				return false;
  			}

  			StringBuffer sb = new StringBuffer();
  			sb.append("select a.city_id,a.DEVICEID,a.DEV_NAME,a.HOST_IP,a.HOST_USER,a.HOST_PWD,a.ENCODE,a.HOST_SIGN,a.DEVICENAME,a.vendor,b.DBDRIVER,b.DBURL,");
  			sb.append("b.GROUP_ID,b.TASK_ID,b.TASK_DESCRIBE,b.DEV_PORT,b.PROXY_DEV_PORT,b.COLLECT_TYPE,b.COLLECT_PERIOD,");
  			sb.append("b.COLLECTTIMEOUT,b.PARSERID,b.DISTRIBUTORID,b.redo_time_offset,b.COLLECT_TIME,b.COLLECT_TIMEPOS,b.PROB_STARTTIME,b.COLLECT_PATH,b.SHELL_CMD_PREPARE,b.SHELL_CMD_FINISH,b.SHELL_TIMEOUT,b.PARSE_TMPID,d.TMPTYPE as TMPTYPE_P,d.TMPNAME as TMPNAME_P,d.EDITION as EDITION_P,d.TEMPFILENAME as TEMPFILENAME_P,b.DISTRBUTE_TMPID,f.tmptype as TMPTYPE_D,f.tmpname as TMPNAME_D,f.edition as EDITION_D,f.tempfilename as TEMPFILENAME_D,");
  			sb.append("b.DISTRBUTE_TMPID,b.SUC_DATA_TIME,b.end_data_time,b.SUC_DATA_POS,b.MAXCLTTIME,b.BLOCKEDTIME,");
  			sb.append("c.DEVICEID as PROXY_DEV_ID,c.DEV_NAME as PROXY_DEV_NAME,c.HOST_IP as PROXY_HOST_IP,c.HOST_USER as PROXY_HOST_USER,c.HOST_PWD as PROXY_HOST_PWD,c.HOST_SIGN as PROXY_HOST_SIGN,b.THREADSLEEPTIME,b.INDBSERVER,b.INDBUSER,b.INDBPASSWORD ");
  			sb.append("from utl_conf_device a,utl_conf_task b left join utl_conf_device c on(b.PROXY_DEV_ID = c.DEVICEID) left join  utl_conf_template d on(b.PARSE_TMPID = d.TMPID) left join  utl_conf_template f on(b.distrbute_tmpid = f.TMPID)");
  			sb.append("where a.DEVICEID = b.DEVICEID and b.ISUSED=1 and b.COLLECTOR_NAME='" + 
  					localHostName + "' ");
  			addIds(sb, 0);

  			sb.append("Order By b.suc_data_time");

  			String strSQL = sb.toString();
  			//log.debug("读取任务表SQL为: " + strSQL);

  			pstmt = conn.prepareStatement(strSQL);
  			rs = pstmt.executeQuery();

  			int i = 0;

  			if (this.tempTasks == null)
  			{
  				this.tempTasks = new ArrayList();
  			}
  			else
  			{
  				this.tempTasks.clear();
  			}
  			CollectObjInfo info;
  			while (rs.next())
  			{
  				try
  				{
  					//构建任务
  					int taskID = rs.getInt("TASK_ID");
  					info = new CollectObjInfo(taskID);

  					info.buildObj(rs, scandate);

  					info.setHostName(localHostName);
  					
  					this.tempTasks.add(info);
  				}
  				catch (Exception e)
  				{
  					log.error("构建任务时异常.原因:", e);
  				}

  				i++;
  			}

  			//探针任务
  			this.tempTasks = DelayProbeMgr.probe(this.tempTasks);
  			if (this.tempTasks != null)
  			{
  				for (CollectObjInfo c : this.tempTasks)
  				{
  					if (c == null)
  						continue;
  					c.startTask();
  					DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(c.getTaskID()));
  				}
  				this.tempTasks.clear();
  				/*
  				for (CollectObjInfo task : this.tempTasks)
  				{
  					if (task == null)
  						continue;
  					addTask(task);
  				}*/

  			}

  			log.debug("从任务表中select出的任务数为: " + i);

  			bReturn = true;
  		}
  		catch (Exception e)
  		{
  			errorlog.error("从任务表中读取任务信息时异常,原因:", e);
  			try
  			{
  				if (rs != null)
  					rs.close();
  				if (pstmt != null)
  					pstmt.close();
  				if (conn != null)
  					conn.close();
  			}
  			catch (Exception localException2)
  			{
  			}
  		}
  		finally
  		{
  			try
  			{
  				if (rs != null)
  					rs.close();
  				if (pstmt != null)
  					pstmt.close();
  				if (conn != null) {
  					conn.close();
  				}
  			}
  			catch (Exception localException3)
  			{
  			}
  		}
  		return bReturn;
  	}

  	/**
  	 * 持续性采集 FTP采集 collect_type = 3，只支持按天采集，补采前一天的数据
  	 * @param scandate
  	 * @return
  	 */
  	private boolean getPersistentCollectInfo(Date scandate)
  	{
  		String localHostName = Util.getHostName();
  		boolean bReturn = false;

  		PreparedStatement pstmt = null;
  		ResultSet rs = null;

  		Connection conn = null;
  		try
  		{
  			log.debug("Starting getConnection...");
  			conn = CommonDB.getConnection();
  			log.debug("GetConnection done...");
  			if (conn == null)
  			{
  				errorlog.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
  				return false;
  			}

  			StringBuffer sb = new StringBuffer();
  			sb.append("select a.city_id,a.DEVICEID,a.DEV_NAME,a.HOST_IP,a.HOST_USER,a.HOST_PWD,a.ENCODE,a.HOST_SIGN,a.DEVICENAME,a.vendor,b.DBDRIVER,b.DBURL,");
  			sb.append("b.GROUP_ID,b.TASK_ID,b.TASK_DESCRIBE,b.DEV_PORT,b.PROXY_DEV_PORT,b.COLLECT_TYPE,b.COLLECT_PERIOD,");
  			sb.append("b.COLLECTTIMEOUT,b.PARSERID,b.DISTRIBUTORID,b.redo_time_offset,b.COLLECT_TIME - 1,b.COLLECT_TIMEPOS,b.PROB_STARTTIME,b.COLLECT_PATH,b.SHELL_CMD_PREPARE,b.SHELL_CMD_FINISH,b.SHELL_TIMEOUT,b.PARSE_TMPID,d.TMPTYPE as TMPTYPE_P,d.TMPNAME as TMPNAME_P,d.EDITION as EDITION_P,d.TEMPFILENAME as TEMPFILENAME_P,b.DISTRBUTE_TMPID,f.tmptype as TMPTYPE_D,f.tmpname as TMPNAME_D,f.edition as EDITION_D,f.tempfilename as TEMPFILENAME_D,");
  			sb.append("b.DISTRBUTE_TMPID,b.SUC_DATA_TIME,b.end_data_time,b.SUC_DATA_POS,b.MAXCLTTIME,b.BLOCKEDTIME,");
  			sb.append("c.DEVICEID as PROXY_DEV_ID,c.DEV_NAME as PROXY_DEV_NAME,c.HOST_IP as PROXY_HOST_IP,c.HOST_USER as PROXY_HOST_USER,c.HOST_PWD as PROXY_HOST_PWD,c.HOST_SIGN as PROXY_HOST_SIGN,b.THREADSLEEPTIME,b.INDBSERVER,b.INDBUSER,b.INDBPASSWORD ");
  			sb.append("from utl_conf_device a,utl_conf_task b left join utl_conf_device c on(b.PROXY_DEV_ID = c.DEVICEID) left join  utl_conf_template d on(b.PARSE_TMPID = d.TMPID) left join  utl_conf_template f on(b.distrbute_tmpid = f.TMPID)");
  			sb.append("where a.DEVICEID = b.DEVICEID and b.collect_type = 3 and b.ISUSED=1 and b.COLLECTOR_NAME='" + 
  					localHostName + "' ");
  			addIds(sb, 0);

  			sb.append("Order By b.suc_data_time");

  			String strSQL = sb.toString();
  			//log.debug("读取任务表SQL为: " + strSQL);

  			pstmt = conn.prepareStatement(strSQL);
  			rs = pstmt.executeQuery();

  			int i = 0;

  			//if (this.tempTasks == null)
  			//{
  			//	this.tempTasks = new ArrayList();
  			//}
  			//else
  			//{
  			//	this.tempTasks.clear();
  			//}
  			CollectObjInfo info;
  			while (rs.next())
  			{
  				try
  				{
  					//构建任务
  					int taskID = rs.getInt("TASK_ID");
  					info = new CollectObjInfo(taskID);
  					info.setPersistentTask(true);
  					info.buildObj(rs, scandate);

  					info.setHostName(localHostName);
  					
  					//this.tempTasks.add(info);
  				}
  				catch (Exception e)
  				{
  					errorlog.error("构建任务时异常.原因:", e);
  				}

  				i++;
  			}

  			//探针任务
  			//this.tempTasks = DelayProbeMgr.probe(this.tempTasks);
  			//if (this.tempTasks != null)
  			//{
  			//	for (CollectObjInfo c : this.tempTasks)
  			//	{
  			//		if (c == null)
  			//			continue;
  			//		c.startTask();
  			//		DelayProbeMgr.getTaskEntrys().remove(Integer.valueOf(c.getTaskID()));
  			//	}
  			//	this.tempTasks.clear();
  				
  				//for (CollectObjInfo task : this.tempTasks)
  				//{
  				//	if (task == null)
  				//		continue;
  				//	addTask(task);
  				//}

  			//}

  			log.debug("从任务表中select出的任务数为: " + i);

  			bReturn = true;
  		}
  		catch (Exception e)
  		{
  			errorlog.error("从任务表中读取任务信息时异常,原因:", e);
  			try
  			{
  				if (rs != null)
  					rs.close();
  				if (pstmt != null)
  					pstmt.close();
  				if (conn != null)
  					conn.close();
  			}
  			catch (Exception localException2)
  			{
  			}
  		}
  		finally
  		{
  			try
  			{
  				if (rs != null)
  					rs.close();
  				if (pstmt != null)
  					pstmt.close();
  				if (conn != null) {
  					conn.close();
  				}
  			}
  			catch (Exception localException3)
  			{
  			}
  		}
  		return bReturn;
  	}
  	
  	private synchronized void addIds(StringBuffer sql, int taskType)
  	{
  		if (taskType == 0)
  		{
  			if (this.activeTasks.isEmpty())
  				return;
  		}
  		if (taskType == 1)
  		{
  			if (this.activeTasksForRegather.isEmpty()) {
  				return;
  			}
  		}
  		List ids = toIDs(taskType);
  		if ((ids == null) || (ids.size() <= 0))
  			return;
  		if (taskType == 0)
  			sql.append(" and b.TASK_ID NOT IN(");
  		else if (taskType == 1) {
  			sql.append(" and e.ID NOT IN(");
  		}
  		int size_1 = ids.size() - 1;
  		for (int i = 0; i < size_1; i++)
  		{
  			sql.append(ids.get(i) + ",");
  		}
  		sql.append(ids.get(size_1));

  		sql.append(") ");
  	}

  	private synchronized List<Integer> toIDs(int type)
  	{
  		List idList = null;

  		Set<Integer> idSet = new HashSet();
  		idSet.addAll(this.activeTasks.keySet());
  		idSet.addAll(this.activeTasksForRegather.keySet());

  		if (idSet.size() > 0) {
  			idList = new ArrayList();
  		}
  		for (Integer id : idSet)
  		{
  			CollectObjInfo obj = getObjByID(id.intValue());
  			isActive(id.intValue(), obj instanceof RegatherObjInfo);
  			if (obj == null) {
  				continue;
  			}
  			if ((type == 1) && ((obj instanceof RegatherObjInfo)))
  			{
  				idList.add(Integer.valueOf(id.intValue() - 10000000));
  			}
  			else {
  				if ((type != 0) || (obj.getKeyID() != obj.getTaskID()))
  					continue;
  				idList.add(id);
  			}
  		}

  		return idList;
  	}

  	public synchronized CollectObjInfo getObjByID(int id)
  	{
  		CollectObjInfo obj = (CollectObjInfo)this.activeTasks.get(Integer.valueOf(id));
  		if (obj == null) {
  			obj = (CollectObjInfo)this.activeTasksForRegather.get(Integer.valueOf(id));
  		}
  		return obj;
  	}

  	private boolean getRegatherInfo()
  	{
  		String localHostName = Util.getHostName();
  		boolean bReturn = false;
  		
  		PreparedStatement pstmt = null;
  		ResultSet rs = null;
  		Connection conn = null;
  		try
  		{
  			log.debug("Starting getConnection for regatherInfo...");
  			conn = CommonDB.getConnection();
  			log.debug("GetConnection for regatherInfo done...");
  			if (conn == null)
  			{
  				log.error("从补采表中读取信息失败,原因:无法获取数据库连接.");
  				return false;
  			}
  			
  			//每次最大补采任务数
  			int maxCountPerRegather = SystemConfig.getInstance().getMaxCountPerRegather();

  			StringBuffer sb = new StringBuffer();
  			sb.append("select * from ");
  			sb.append("(select topflag (e.ID + 10000000) as ID,e.taskid,e.filepath,e.collecttime,e.readopttype,e.collectdegress,e.collectstatus,e.collector_name,e.stamptime,c.city_id,c.DEVICEID,c.DEV_NAME,c.ENCODE,c.HOST_IP,c.HOST_USER,c.HOST_PWD,c.HOST_SIGN,c.DEVICENAME,c.vendor,b.DBDRIVER,b.DBURL,");
  			sb.append("b.GROUP_ID,b.TASK_ID,b.TASK_DESCRIBE,b.DEV_PORT,b.PROXY_DEV_PORT,b.COLLECT_TYPE,b.COLLECT_PERIOD,");
  			sb.append("b.COLLECTTIMEOUT,b.COLLECT_TIME,b.PROB_STARTTIME,b.COLLECT_TIMEPOS,b.COLLECT_PATH,b.SHELL_CMD_PREPARE,b.SHELL_CMD_FINISH,b.SHELL_TIMEOUT,b.PARSE_TMPID,d.TMPTYPE as TMPTYPE_P,d.TMPNAME as TMPNAME_P,d.EDITION as EDITION_P,d.TEMPFILENAME as TEMPFILENAME_P,b.DISTRBUTE_TMPID,f.tmptype as TMPTYPE_D,f.tmpname as TMPNAME_D,f.edition as EDITION_D,f.tempfilename as TEMPFILENAME_D,");
  			sb.append("b.PARSERID,b.DISTRIBUTORID,b.redo_time_offset,b.SUC_DATA_TIME,b.end_data_time,b.SUC_DATA_POS,b.MAXCLTTIME,b.BLOCKEDTIME,");
  			sb.append("c.DEVICEID as PROXY_DEV_ID,c.DEV_NAME as PROXY_DEV_NAME,c.HOST_IP as PROXY_HOST_IP,c.HOST_USER as PROXY_HOST_USER,c.HOST_PWD as PROXY_HOST_PWD,c.HOST_SIGN as PROXY_HOST_SIGN,b.THREADSLEEPTIME,b.INDBSERVER,b.INDBUSER,b.INDBPASSWORD ");
  			sb.append("from utl_conf_rtask e left join utl_conf_task b on e.taskid = b.task_id left join utl_conf_device c  on(b.DEVICEID = c.DEVICEID) left join utl_conf_template d on (d.tmpid=b.parse_tmpid) left join utl_conf_template f on (f.tmpid=b.distrbute_tmpid) ");
  			sb.append("where b.ISUSED=1 ");

  			String strCondition = " e.COLLECTOR_NAME='" + localHostName + "' ";

  			String strTemp = "";
  			if (Util.isOracle())
  			{
  				sb.append(" and " + strCondition);
  				sb.append(" and e.COLLECTSTATUS=0 ");
  				addIds(sb, 1);

  				sb.append(" order by e.READOPTTYPE desc,e.COLLECTTIME desc) ");
  				sb.append("where rownum <=" + maxCountPerRegather);
  			}
  			else if (Util.isSybase())
  			{
  				strTemp = "top " + maxCountPerRegather;
  				sb.append(" and " + strCondition + " and e.COLLECTSTATUS=0 ");
  				addIds(sb, 1);
  				sb.append("order by e.READOPTTYPE desc ,e.COLLECTTIME desc) ");
  				sb.append("where rownum <=" + maxCountPerRegather);
  			}
  			else if(Util.isMySQL())
  			{
  				//strTemp = "top " + maxCountPerRegather;
  				sb.append(" and " + strCondition + " and e.COLLECTSTATUS=0 ");
  				addIds(sb, 1);
  				sb.append("order by e.READOPTTYPE desc ,e.COLLECTTIME desc) as a limit 200");
  				//sb.append("where rownum <=" + maxCountPerRegather);
  			}

  			String strSQL = sb.toString();
  			strSQL = strSQL.replaceFirst("topflag", strTemp);

  			log.debug("补采任务SQL为: " + strSQL);

  			pstmt = conn.prepareStatement(strSQL);
  			rs = pstmt.executeQuery();

  			int i = 0;
  			while (rs.next())
  			{
  				try
  				{
  					int ID = rs.getInt("ID");
  					RegatherObjInfo info = new RegatherObjInfo(ID, rs.getInt("taskid"));

  					info.buildObj(rs, new Date());

  					info.setHostName(localHostName);
  					i++;
  				}
  				catch (Exception e)
  				{
  					log.error("构建补采任务时异常,原因:", e);
  				}
  			}

  			log.debug("从补采表中select出的补采任务数为: " + i);

  			bReturn = true;
  		}
  		catch (Exception e)
  		{
  			errorlog.error("从补采表中读取补采信息时异常,原因:", e);
  		}
  		finally
  		{
  			CommonDB.close(rs, pstmt, conn);
  		}
  		return bReturn;
  	}

  	/**
  	 * 更新补采任务状态
  	 * @param state
  	 * @param id
  	 * @return
  	 */
  	public boolean updateRegatherState(int state, int id)
  	{
  		int ret = -1;
  		String strSQL = "update UTL_CONF_RTASK  set collectstatus=" + state + " where ID = " + id;
  		try
  		{
  			ret = CommonDB.executeUpdate(strSQL);
  		}
  		catch (SQLException e)
  		{
  			errorlog.error("R-Task-" + id + ": 更新collectstatus字段为" + state + "时异常,原因:", e);
  		}

  		return ret >= 0;
  	}

  	public static class RedoSQL
  	{
  		public String sql;
  		public String cause;

  		public RedoSQL(String sql, String cause)
  		{
  			this.sql = sql;
  			this.cause = cause;
  		}
  	}
  	
  	public void StopSendTask()
  	{
  		this.checkFlag = false;
  	}

  	class RegatherStruct
  	{
  		private int key;
  		private int taskID;
  		private List<String> ds;
  		private Timestamp dataTime;
  		private String cause;

  		public RegatherStruct()
  		{
    	}

  		public RegatherStruct(int key,int taskID, List<String> ds, Timestamp dataTime,String cause)
  		{
  			this.key = key;
  			this.taskID = taskID;
  			this.ds = ds;
  			this.dataTime = dataTime;
  			this.cause = cause;
  		}

  		public boolean dsExists(String strDS)
  		{
  			if (this.ds == null) {
  				return false;
  			}
  			return this.ds.contains(strDS);
  		}

  		public void addDS(String strDS)
  		{
  			if (this.ds == null) {
  				return;
  			}
  			if (this.ds.contains(strDS)) {
  				return;
  			}
  			this.ds.add(strDS);
  		}

  		public int getKey()
  		{
  			return this.key;
  		}

  		public void setKey(int key)
  		{
  			this.key = key;
  		}

  		public int getTaskID()
  		{
  			return this.taskID;
  		}

  		public void setTaskID(int taskID)
  		{
  			this.taskID = taskID;
  		}

  		public List<String> getDs()
  		{
  			return this.ds;
  		}

  		public void setDs(List<String> ds)
  		{
  			this.ds = ds;
  		}

  		public Timestamp getDataTime()
  		{
  			return this.dataTime;
  		}

  		public void setDataTime(Timestamp dataTime)
  		{
  			this.dataTime = dataTime;
  		}

  		public String getCause()
  		{
  			return this.cause;
  		}

  		public void setCause(String cause)
  		{
  			this.cause = cause;
  		}
  	}

  	private static class TaskMgrContainer
	{
  		private static TaskMgr instance = new TaskMgr();
	}
}