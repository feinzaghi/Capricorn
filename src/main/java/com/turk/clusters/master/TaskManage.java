package com.turk.clusters.master;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.turk.clusters.common.ReTaskObjInfo;
import com.turk.clusters.common.TaskObjInfo;
import com.turk.clusters.model.Register;
import com.turk.clusters.model.SlaveInfo;
import com.turk.clusters.model.StatTaskInfo;

import com.turk.Config.SystemConfig;
import com.turk.util.CommonDB;
import com.turk.util.Util;
import com.turk.util.LogMgr;

/**
 * 
 * @author Administrator
 * 
 * 主控通过读取配置表，组装任务信息，将任务已json格式发送到节点
 */
public class TaskManage {
	
	private static TaskManage _instance = null;
	private Logger log = LogMgr.getInstance().getAppLogger("master");
	private Logger errorlog = LogMgr.getInstance().getErrorLogger();
	
	//注册节点信息
	private Map<String,SlaveInfo> _nodes = new HashMap<String, SlaveInfo>();
	
	/**
	 * 活动任务
	 */
	private Map<Integer, TaskObjInfo> activeTasks;
	
	/**
	 * 活动汇总任务
	 */
	private Map<Integer, StatTaskInfo> activeStatTasks;
	
	/**
	 * 活动补采任务
	 */
	private Map<Integer, ReTaskObjInfo> activeTasksForRegather;
	
	/**
	 * 标志位检查
	 */
	private boolean checkFlag = true;
	
	/**
	 * 
	 */
	//public List<TaskObjInfo> tempTasks = new ArrayList<TaskObjInfo>();
	
	/**
	 * 
	 */
	public List<StatTaskInfo> tempStatTasks = new ArrayList<StatTaskInfo>();
	
	public static TaskManage getInstance()
	{
		if(_instance == null)
		{
			_instance = new TaskManage();
		}
		return _instance;
	}
	
	public TaskManage()
	{
		this.activeTasks = new HashMap<Integer, TaskObjInfo>();//活动采集任务
		this.activeTasksForRegather = new HashMap<Integer, ReTaskObjInfo>();//活动补采任务
	}
	
	/**
	 * 登记节点信息
	 * @param reg
	 */
	public synchronized void NodeRegister(Register reg)
	{
		if(!_nodes.containsKey(reg.getServer()))
		{
			SlaveInfo info = new SlaveInfo();
			info.setServer(reg.getServer());
			info.setPort(reg.getPort());
			info.setStatus(reg.getFlag());
			info.setCurrentCltCount(reg.getCurrentCltCount());
			info.setMaxCltCount(reg.getMaxCltCount());
			info.setMaxActiveTask(reg.getMaxActiveTask());
			info.setActiveTime(new Date());
			_nodes.put(reg.getServer(), info);
			log.debug("Slave 注册:IP[" + reg.getServer() + "]");
		}
	}
	
	
	/**
	 * 在主节点更新节点状态
	 * @param reg
	 */
	public synchronized void UpdateSlaveStatus(Register reg)
	{
		if(_nodes.containsKey(reg.getServer()))
		{
			SlaveInfo info = _nodes.get(reg.getServer());
			info.setStatus(reg.getFlag());
			info.setActiveTime(new Date());
			info.setCurrentCltCount(reg.getCurrentCltCount());
			//info.setMaxCltCount(reg.getMaxCltCount());
			//info.setMaxActiveTask(reg.getMaxActiveTask());
			log.debug("Slave:IP[" + reg.getServer() + "] Normal!");
		}
		else //不存在，重新注册
		{
			SlaveInfo info = new SlaveInfo();
			info.setServer(reg.getServer());
			info.setPort(reg.getPort());
			info.setStatus(reg.getFlag());
			info.setCurrentCltCount(reg.getCurrentCltCount());
			info.setMaxCltCount(reg.getMaxCltCount());
			info.setMaxActiveTask(reg.getMaxActiveTask());
			info.setActiveTime(new Date());
			_nodes.put(reg.getServer(), info);
			log.debug("Slave 注册:IP[" + reg.getServer() + "]");
		}
	}
	
	public synchronized Map<String,SlaveInfo> getSlaves()
	{
		return this._nodes;
	}
	
	public synchronized SlaveInfo getSlaves(String server)
	{
		return this._nodes.get(server);
	}
	
	/**
	 * 当前活动线程数
	 * @return
	 */
	public synchronized int size()
	{
		return this.activeTasks.size() + this.activeTasksForRegather.size();
	}

	
	/**
  	 * 扫描正常采集任务
  	 * @param scanDate 扫描时间
  	 * @return
  	 */
  	public boolean loadNormalTasksFromDB(Date scanDate)
  	{
  		if (!this.checkFlag) {
  			return false;
  		}
  		log.debug("开始加载任务信息...");

  		boolean bReturn = getCollectInfo(scanDate);

  		log.debug("load tasks from DB. --Done(" + bReturn + ")");

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
  	
  	/**
  	 * 扫描采集信息
  	 * @param scandate
  	 * @return
  	 */
  	private boolean getCollectInfo(Date scandate)
  	{
  		String localHostName = Util.getHostName();
  		boolean bReturn = false;

  		PreparedStatement pstmt = null;
  		ResultSet rs = null;

  		/*if (SystemConfig.getInstance().getMRProcessId() != 0) {
  			localHostName = localHostName + "@" + SystemConfig.getInstance().getMRProcessId();
  		}*/
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

  			//扫描所有待加载任务
  			StringBuffer sb = new StringBuffer();
  			sb.append("select a.city_id,a.DEVICEID,a.DEV_NAME,a.HOST_IP,a.HOST_USER,a.HOST_PWD,a.ENCODE,a.HOST_SIGN,a.DEVICENAME,a.vendor,b.DBDRIVER,b.DBURL,");
  			sb.append("b.GROUP_ID,b.TASK_ID,b.TASK_DESCRIBE,b.DEV_PORT,b.PROXY_DEV_PORT,b.COLLECT_TYPE,b.COLLECT_PERIOD,");
  			sb.append("b.COLLECTTIMEOUT,b.PARSERID,b.DISTRIBUTORID,b.redo_time_offset,b.COLLECT_TIME,b.COLLECT_TIMEPOS,b.PROB_STARTTIME,b.COLLECT_PATH,b.SHELL_CMD_PREPARE,b.SHELL_CMD_FINISH,b.SHELL_TIMEOUT,b.PARSE_TMPID,d.TMPTYPE as TMPTYPE_P,d.TMPNAME as TMPNAME_P,d.EDITION as EDITION_P,d.TEMPFILENAME as TEMPFILENAME_P,b.DISTRBUTE_TMPID,f.tmptype as TMPTYPE_D,f.tmpname as TMPNAME_D,f.edition as EDITION_D,f.tempfilename as TEMPFILENAME_D,");
  			sb.append("b.DISTRBUTE_TMPID,b.COLLECTOR_NAME,b.SUC_DATA_TIME,b.end_data_time,b.SUC_DATA_POS,b.MAXCLTTIME,b.BLOCKEDTIME,");
  			sb.append("c.DEVICEID as PROXY_DEV_ID,c.DEV_NAME as PROXY_DEV_NAME,c.HOST_IP as PROXY_HOST_IP,c.HOST_USER as PROXY_HOST_USER,c.HOST_PWD as PROXY_HOST_PWD,c.HOST_SIGN as PROXY_HOST_SIGN,b.THREADSLEEPTIME,b.INDBSERVER,b.INDBUSER,b.INDBPASSWORD ");
  			sb.append("from utl_conf_device a,utl_conf_task b left join utl_conf_device c on(b.PROXY_DEV_ID = c.DEVICEID) left join  utl_conf_template d on(b.PARSE_TMPID = d.TMPID) left join  utl_conf_template f on(b.distrbute_tmpid = f.TMPID)");
  			sb.append("where a.DEVICEID = b.DEVICEID and b.ISUSED=1");
  			addIds(sb, 0);
  			sb.append("Order By b.suc_data_time");

  			String strSQL = sb.toString();
  			log.debug("读取任务表SQL为: " + strSQL);

  			pstmt = conn.prepareStatement(strSQL);
  			rs = pstmt.executeQuery();

  			int i = 0;

  			/*
  			if (this.tempTasks == null)
  			{
  				this.tempTasks = new ArrayList<TaskObjInfo>();
  			}
  			else
  			{
  				TaskMgr.getInstance().tempTasks.clear();
  				this.tempTasks.clear();
  			}*/
  			TaskObjInfo info;
  			while (rs.next())
  			{
  				try
  				{
  					//选取最优的节点分派任务
  					SlaveInfo slave = null;
  					String collector = rs.getString("COLLECTOR_NAME");
  					if(collector == null)
  					{
  						slave = getExecuteSlave();
  					}
  					else
  					{
  						slave = getExecuteSlave(collector);
  					}
  					if(slave == null)
  					{
  						log.debug("Get slave:null");
  						continue;
  					}
  					//构建任务
  					
  					int taskID = rs.getInt("TASK_ID");
  					log.debug("构建任务："+taskID);
  					info = new TaskObjInfo(taskID,slave.getServer(),slave.getPort());
  					info.setKeyID(taskID);
  					info.setHostName(localHostName);
  					info.buildObj(rs, scandate);
  					//this.tempTasks.add(info);
  				}
  				catch (Exception e)
  				{
  					log.error("构建任务时异常.原因:", e);
  				}

  				i++;
  			}

  			log.debug("从任务表中select出的任务数为: " + i);

  			bReturn = true;
  		}
  		catch (Exception e)
  		{
  			log.error("从任务表中读取任务信息时异常,原因:", e);
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
  	 * 获取补采任务信息
  	 * @return
  	 */
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

  			//String strCondition = " e.COLLECTOR_NAME='" + localHostName + "' ";

  			String strTemp = "";
  			if (Util.isOracle())
  			{
  				//sb.append(" and " + strCondition);
  				sb.append(" and e.COLLECTSTATUS=0 ");
  				addIds(sb, 1);

  				sb.append(" order by e.READOPTTYPE desc,e.COLLECTTIME desc) ");
  				sb.append("where rownum <=" + maxCountPerRegather);
  			}
  			else if (Util.isSybase())
  			{
  				strTemp = "top " + maxCountPerRegather;
  				//sb.append(" and " + strCondition + " and e.COLLECTSTATUS=0 ");
  				addIds(sb, 1);
  				sb.append("order by e.READOPTTYPE desc ,e.COLLECTTIME desc) ");
  				sb.append("where rownum <=" + maxCountPerRegather);
  			}
  			else if(Util.isMySQL())
  			{
  				//strTemp = "top " + maxCountPerRegather;
  				//sb.append(" and " + strCondition + " and e.COLLECTSTATUS=0 ");
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
  					ReTaskObjInfo info = new ReTaskObjInfo(ID, rs.getInt("taskid"));
  					info.setKeyID(ID);
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
  	 * 获取最优执行的节点信息
  	 * @return
  	 */
  	public SlaveInfo getExecuteSlave()
  	{
  		//Map<Integer,String> map = new HashMap<Integer, String>();
  		SlaveInfo info = null;
  		int max = 0;
  		String server = "";
  		for(SlaveInfo slave : this._nodes.values())
  		{
  			if(slave.getStatus()!=1)
  				continue;
  			//得到活动线程占的比例，越大越忙
  			
  			//如果已达到最大线程数，则不派发任务
  			if(slave.getMaxActiveTask()<slave.getCurrentCltCount())
  				continue;
  			
  			int current = slave.getMaxActiveTask() - slave.getCurrentCltCount();
  			//得到线程差值，值越大说明越空闲
  			//if(!map.containsKey(rate))
  			//	map.put(rate, slave.getServer());
  			//int value = slave.getMaxActiveTask() - slave.getCurrentCltCount();
  			log.debug("Slave:" + slave.getServer()+ " MaxClt:" + slave.getMaxActiveTask()
  					+ " CurrentClt:" + slave.getCurrentCltCount());
  			if(current > max)
  			{
  				max = current;
  				server = slave.getServer();//得到N个节点中的最大值
  			}
  		}
  		if(!server.isEmpty())
  		{
  			info = this._nodes.get(server);
  			if(info == null)
  				return null;
  			String strMsg = String.format("Get Server:%s",server);
  			log.debug(strMsg);
  		}
  		//map = null;
  		return info;
  	}
  	
  	/**
  	 * 指定运行服务器，如果该服务器不在线，则选择最优服务器执行
  	 * @return
  	 */
  	private SlaveInfo getExecuteSlave(String machinename)
  	{
  		SlaveInfo info = null;

  		try {
				
  			StringBuilder strIp = new StringBuilder();
			byte[] ip = InetAddress.getByName(machinename).getAddress();
			for (int i=0;i<ip.length;i++){
				   if (i>0) strIp.append(".");
				   strIp.append(ip[i]&0xFF);
		    }
			for(SlaveInfo slave : this._nodes.values())
	  		{
	  			if(slave.getServer().equals(strIp.toString()) 
	  					&& (slave.getStatus() == 4 || slave.getStatus() == 1))
	  			{
	  				info = slave;
	  				break;
	  			}
	  		}
			
			//若当前使用的节点不正常，则优选一个节点
			//if(info!=null && info.getStatus()!=1 && info.getStatus()!=4)
			//	return getExecuteSlave();
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			//e.printStackTrace();
			errorlog.error("解析机器名异常",e);
		}
  		
  		return info;
  	}
  	
  	public boolean loadStatTaskFromDB(Date scanDate)
  	{
  		if (!this.checkFlag) {
  			return false;
  		}
  		log.debug("开始加载任务信息...");

  		boolean bReturn = getStatTaskInfo(scanDate);

  		log.debug("load tasks from DB. --Done(" + bReturn + ")");

  		return bReturn;
  	}
  	
  	/**
  	 * 扫描采集信息
  	 * @param scandate
  	 * @return
  	 */
  	private boolean getStatTaskInfo(Date scandate)
  	{
  		boolean bReturn = false;

  		PreparedStatement pstmt = null;
  		ResultSet rs = null;

  		/*if (SystemConfig.getInstance().getMRProcessId() != 0) {
  			localHostName = localHostName + "@" + SystemConfig.getInstance().getMRProcessId();
  		}*/
  		Connection conn = null;
  		try
  		{
  			log.debug("Starting getConnection...");
  			conn = CommonDB.getConnection();
  			log.debug("GetConnection done...");
  			if (conn == null)
  			{
  				log.error("从汇总任务表中读取信息失败,原因:无法获取数据库连接.");
  				return false;
  			}

  			//扫描所有待加载任务
  			StringBuffer sb = new StringBuffer();
  			sb.append("select id,task_name,start_time,ftpip,ftppath,ftpuser,ftppwd,task_type,stat_type ");
  			sb.append("from utl_conf_stat_task  where isused = 1");
  			sb.append("Order By start_time");

  			String strSQL = sb.toString();
  			log.debug("读取任务表SQL为: " + strSQL);

  			pstmt = conn.prepareStatement(strSQL);
  			rs = pstmt.executeQuery();

  			int i = 0;

  			if (this.tempStatTasks == null)
  			{
  				this.tempStatTasks = new ArrayList<StatTaskInfo>();
  			}
  			else
  			{
  				this.tempStatTasks.clear();
  			}
  			StatTaskInfo info;
  			while (rs.next())
  			{
  				try
  				{
  					//选取最优的节点分派任务
  					SlaveInfo slave = getExecuteSlave();
  					if(slave == null)
  						continue;
  					info = new StatTaskInfo();
  					info.buildObj(rs, scandate);
  					//info.setHostName(localHostName);
  					
  					this.tempStatTasks.add(info);
  				}
  				catch (Exception e)
  				{
  					log.error("构建任务时异常.原因:", e);
  				}

  				i++;
  			}

  			//探针任务
  			/*this.tempTasks = DelayProbeMgr.probe(this.tempTasks);
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
  				
  				//for (CollectObjInfo task : this.tempTasks)
  				//{
  				//	if (task == null)
  				//		continue;
  				//	addTask(task);
  				//}

  			}*/

  			log.debug("从任务表中select出的任务数为: " + i);

  			bReturn = true;
  		}
  		catch (Exception e)
  		{
  			log.error("从任务表中读取任务信息时异常,原因:", e);
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
  		List<Integer> ids = toIDs(taskType);
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
  		List<Integer> idList = null;

  		Set<Integer> idSet = new HashSet<Integer>();
  		idSet.addAll(this.activeTasks.keySet());
  		idSet.addAll(this.activeTasksForRegather.keySet());

  		if (idSet.size() > 0) {
  			idList = new ArrayList<Integer>();
  		}
  		for (Integer id : idSet)
  		{
  			TaskObjInfo obj = getObjByID(id.intValue());
  			isActive(id.intValue(), obj instanceof ReTaskObjInfo);
  			if (obj == null) {
  				continue;
  			}
  			if ((type == 1) && ((obj instanceof ReTaskObjInfo)))
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
  	
  	public synchronized TaskObjInfo getObjByID(int id)
  	{
  		TaskObjInfo obj = (TaskObjInfo)this.activeTasks.get(Integer.valueOf(id));
  		if (obj == null) {
  			obj = (TaskObjInfo)this.activeTasksForRegather.get(Integer.valueOf(id));
  		}
  		return obj;
  	}
  	
  	/**
	 * 根据任务号检查该任务是否为活动任务
	 * @param taskID 任务编号
	 * @param isReclt 是否为补采任务
	 * @return
	 */
	public synchronized boolean isActive(int taskID, boolean isReclt)
	{
		Map<Integer, ? extends TaskObjInfo> map = isReclt ? this.activeTasksForRegather : this.activeTasks;
		boolean bExist = map.containsKey(Integer.valueOf(taskID));

		/*
		if (bExist)
		{
			TaskObjInfo cltobj = (TaskObjInfo)map.get(Integer.valueOf(taskID));

			int iBlockedTime = cltobj.getBlockedTime();
			long currTime = System.currentTimeMillis();

			if ((iBlockedTime != 0) && 
					((currTime - cltobj.startTime.getTime()) / 1000L / 60L >= iBlockedTime))
			{
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
	 * 根据任务号检查该任务是否为活动任务
	 * @param taskID 任务编号
	 * @param isReclt 是否为补采任务
	 * @return
	 */
	public synchronized boolean isStatActive(int taskID)
	{
		Map<Integer, StatTaskInfo> map = this.activeStatTasks;
		boolean bExist = map.containsKey(Integer.valueOf(taskID));
		return bExist;
	}

	/**
	 * 删除活动线程
	 * @param taskID
	 * @param isReclt
	 */
	public synchronized void delActiveTask(int taskID, boolean isReclt)
	{
		Map<Integer, ? extends TaskObjInfo> map = isReclt ? this.activeTasksForRegather : this.activeTasks;
		if (map.containsKey(Integer.valueOf(taskID)))
			map.remove(Integer.valueOf(taskID));
	}
	
	/**
	 * 添加任务
	 * @param obj
	 * @return
	 */
	public synchronized boolean addTask(TaskObjInfo obj)
	{
		if (obj == null) {
			return false;
		}

		//检查是否为补采任务
		boolean isReclt = obj instanceof ReTaskObjInfo;
		
		int keyID = obj.getKeyID();
		//2013-06-21 不限制最大线程数
		//if ((!isActive(keyID, isReclt)) && (!isMaxThreadCount(isReclt)))
		{
			if (isReclt)
			{
				this.activeTasksForRegather.put(Integer.valueOf(keyID), (ReTaskObjInfo)obj);
			}
			else
			{
				this.activeTasks.put(Integer.valueOf(keyID), obj);
			}
			return true;
		}

		//return false;
	}
	
	/**
	 * 添加汇总任务
	 * @param obj
	 * @return
	 */
	public synchronized boolean addStatTask(StatTaskInfo obj)
	{
		if (obj == null) {
			return false;
		}

	
		int keyID = obj.getID();
		if ((!isStatActive(keyID)))
		{
			this.activeStatTasks.put(Integer.valueOf(keyID), obj);
			return true;
		}

		return false;
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
  			log.error("R-Task-" + id + ": 更新collectstatus字段为" + state + "时异常,原因:", e);
  		}

  		return ret >= 0;
  	}
	
	public void StopSendTask()
  	{
  		this.checkFlag = false;
  	}
	
	public boolean IsSlaves()
	{
		if(_nodes.size() > 0)
		{
			return true;
		}
		else
		{
			return false;
		}
	}
	
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
