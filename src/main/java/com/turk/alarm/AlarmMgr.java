package com.turk.alarm;

import com.turk.Config.SystemConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.MsgQueue;
import com.turk.util.Util;

/**
 * 平台告警管理
 * @author Administrator
 *
 */
public class AlarmMgr
{
	private AlarmSender alarmSender;
	private static AlarmMgr instance = null;
	private MsgQueue<Alarm> alarmQ;
	private Scanner scanner;
	private ExecutorService executorService;
	private static final Map<Byte, AlarmReSendRule> rules;
  	private static String ALARM_SQL = "INSERT INTO UTL_DATA_ALARM(ID,ALARMLEVEL,TITLE,SRC,DESCRIPTION,OCCUREDTIME,TS,ERRORCODE,TASKID) VALUES(SEQ_UTL_DATA_ALARM.nextval,%s,'%s','%s','%s',to_date('%s','YYYY-MM-DD HH24:MI:SS'),sysdate,%s,%s)";
  	private boolean enable = true;
  	private SystemConfig config = SystemConfig.getInstance();
  	private List<RuleFilter> filters = null;
  	private Logger log = LogMgr.getInstance().getSystemLogger();
  	

  	static
  	{
  		rules = new HashMap<Byte, AlarmReSendRule>();
  		rules.put(Byte.valueOf("0"), new AlarmReSendRule(1, 5));
  		rules.put(Byte.valueOf("1"), new AlarmReSendRule(1, 5));
  		rules.put(Byte.valueOf("2"), new AlarmReSendRule(2, 5));
  		rules.put(Byte.valueOf("3"), new AlarmReSendRule(3, 5));
  		rules.put(Byte.valueOf("4"), new AlarmReSendRule(4, 5));
  		rules.put(Byte.valueOf("5"), new AlarmReSendRule(5, 10));
  	}

  	private AlarmMgr()
  	{
  		this.enable = this.config.isEnableAlarm();
  		if (!this.enable) {
  			return;
  		}

  		init();

  		this.alarmQ = new MsgQueue<Alarm>();

  		this.scanner = new Scanner("Alarm-Scanner-Thrd");
  		this.scanner.setName("Alarm-Scanner-Thrd");
  		this.scanner.start();

  		this.executorService = Executors.newFixedThreadPool(2);
  		for (int i = 0; i < 2; i++)
  			this.executorService.execute(new Sender());
  	}

  	public static AlarmMgr getInstance()
  	{
  		if (instance == null)
  		{
  			synchronized (AlarmMgr.class)
  			{
  				if (instance == null)
  				{
  					instance = new AlarmMgr();
  				}
  			}
  		}
  		return instance;
  	}

  	public void shutdown()
  	{
  		if (this.alarmQ != null) {
  			this.alarmQ.clear();
  		}

  		if (this.scanner != null)
  		{
  			this.scanner.shutdown();
  			this.scanner = null;
  		}

  		if (this.executorService != null)
  			this.executorService.shutdown();
  	}

  	private void init()
  	{
  		loadSender();

  		loadFilters();

  		createTable();

  		createSeq();
  	}

  	private void loadSender()
  	{
  		String sender = this.config.getSender();
  		if (Util.isNotNull(sender))
  		{
  			try
  			{
  				this.alarmSender = ((AlarmSender)Class.forName(sender).newInstance());
  			}
  			catch (Exception e)
  			{
  				log.error("加载告警发送类时发生异常！" + sender, e);
  			}
  		}
  	}

  	private void loadFilters()
  	{
  		List<String> filList = this.config.getFilters();
  		if ((filList != null) && (!filList.isEmpty()))
  		{
  			this.filters = new ArrayList<RuleFilter>();
  			for (String fi : filList)
  			{
  				try
  				{
  					this.filters.add((RuleFilter)Class.forName(fi).newInstance());
  				}
  				catch (Exception e)
  				{
  					log.error("加载过滤类时发生异常！" + fi, e);
  				}
  			}
  		}
  	}

  	/**
  	 * 创建表
  	 */
  	private void createTable()
  	{
  		StringBuilder sb = new StringBuilder("CREATE TABLE UTL_DATA_ALARM(");
  		sb.append("ID NUMBER NOT NULL ENABLE,");
  		sb.append("ALARMLEVEL NUMBER DEFAULT 1 NOT NULL ENABLE,");
  		sb.append("TITLE VARCHAR2(255) NOT NULL ENABLE,");
  		sb.append("SRC VARCHAR2(255) NOT NULL ENABLE,");
  		sb.append("STATUS NUMBER DEFAULT 0 NOT NULL ENABLE,");
  		sb.append("DESCRIPTION VARCHAR2(1000),");
  		sb.append("OCCUREDTIME DATE NOT NULL ENABLE,");
  		sb.append("PROCESSEDTIME DATE,TS DATE,");
  		sb.append("ERRORCODE NUMBER,TASKID NUMBER,");
  		sb.append("SENTTIMES NUMBER DEFAULT 0 NOT NULL ENABLE)");
  		try
  		{
  			CommonDB.executeUpdate(sb.toString());
  		}
  		catch (SQLException e)
  		{
  			if (e.getErrorCode() == 955)
  			{
  				return;
  			}
  			log.error("创建表UTL_DATA_ALARM异常！" + sb, e);
  		}
  	}

  	private void createSeq()
  	{
  		String seq = "create sequence SEQ_UTL_DATA_ALARM start with 1 increment by 1 nocycle";
  		try
  		{
  			CommonDB.executeUpdate(seq);
  		}
  		catch (SQLException e)
  		{
  			if (e.getErrorCode() == 955)
  			{
  				return;
  			}
  			log.error("创建序列SEQ_UTL_DATA_ALARM 异常！" + seq, e);
  		}
  	}

  	
  	/**
  	 * 添加告警 
  	 * @param taskID
  	 * @param level
  	 * @param title
  	 * @param source
  	 * @param description
  	 * @param errorCode
  	 * @param occuredTime
  	 */
  	public void insert(int taskID, byte level, String title, String source, String description, int errorCode, Date occuredTime)
  	{
  		if (!this.enable) {
  			return;
  		}
  		Alarm a = toAlarm(taskID, level, title, source, description, errorCode, occuredTime);
  		if (!filter(a)) return;
  		String sql = String.format(ALARM_SQL, new Object[] { 
  				Byte.valueOf(level), a.getTitle(), a.getSource(), 
  				a.getDescription(), Util.getDateString(occuredTime), 
  				Integer.valueOf(errorCode), Integer.valueOf(taskID) 
  				});
  		int result = -1;
  		try
  		{
  			result = CommonDB.executeUpdate(sql);
  		}
  		catch (SQLException e)
  		{
  			log.error("告警添加失败！" + sql, e);
  		}
  		if (result != -1)
  		{
  			log.debug("告警添加成功！");
  		}
  	}

  	/**
  	 * 添加告警  默认告警级别 0
  	 * @param taskID 任务ID
  	 * @param title 告警标题
  	 * @param source 告警源
  	 * @param description 告警描述
  	 * @param errorCode 异常编号
  	 * @param occuredTime 告警时间
  	 */
  	public void insert(int taskID, String title, String source, String description, int errorCode, Date occuredTime)
  	{
  		insert(taskID, (byte)0, title, source, description, errorCode, occuredTime);
  	}

  	public void insert(int taskID, String title, String source, String description, int errorCode)
  	{
  		insert(taskID, title, source, description, errorCode, new Date());
  	}

  	public void insert(int taskID, byte level, String title, String source, String description, int errorCode)
  	{
  		insert(taskID, level, title, source, description, errorCode, new Date());
  	}

  	public void insert(Alarm alarm)
  	{
  		insert(alarm.getTaskID(), alarm.getAlarmLevel(), alarm.getTitle(), alarm.getSource(), alarm.getDescription(), alarm.getErrorCode(), alarm.getOccuredTime());
  	}

  	private boolean filter(Alarm alarm)
  	{
  		boolean flag = true;
  		if (this.filters != null)
  		{
  			for (RuleFilter rule : this.filters)
  			{
  				if (rule.doFilter(alarm))
  					continue;
  				flag = false;
  				break;
  			}
  		}

  		return flag;
  	}

  	private Alarm toAlarm(int taskID, byte level, String title, String source, String description, int errorCode, Date occuredTime)
  	{
  		Alarm alarm = new Alarm();
  		alarm.setTaskID(taskID);
  		alarm.setAlarmLevel(level);
  		alarm.setTitle(title);
  		alarm.setSource(source);
  		alarm.setDescription(description);
  		alarm.setErrorCode(errorCode);
  		alarm.setOccuredTime(occuredTime);
  		return alarm;
  	}

  	public static void main(String[] args)
  	{
  	}

  	class Scanner extends Thread
  	{
  		private boolean runFlag = true;

  		private String thisName = "";
  		public Scanner()
  		{
  		}

  		public Scanner(String name)
  		{
  			super();
  			thisName = name;
  		}
  		
  		public String toString()
  		{
  			return thisName;
  		}

  		public synchronized boolean isRunning()
  		{
  			return this.runFlag;
  		}

  		public synchronized void shutdown()
  		{
  			this.runFlag = false;
  		}

  		private void builderAlarm(ResultSet rs) throws Exception
  		{
  			if (rs == null) return;
  			Date now = new Date();

  			while (rs.next())
  			{
  				long id = rs.getLong("ID");
  				byte alarmLevel = rs.getByte("ALARMLEVEL");
  				int sentTimes = rs.getInt("SENTTIMES");
  				byte status = rs.getByte("STATUS");
  				Date ts = rs.getDate("TS");
  				AlarmReSendRule reSendRule = (AlarmReSendRule)AlarmMgr.rules.get(Byte.valueOf(alarmLevel));
  				if (reSendRule == null)
  				{
  					log.error("重发规则中不含有此级别：" + alarmLevel);
  				}
  				else {
  					Alarm alarm = null;

  					if (((reSendRule.getMaxReSendTimes() > sentTimes) && (now.getTime() - ts.getTime() < reSendRule.getTimeout() * 60 * 1000)) || 
  							(status == 0))
  					{
  						alarm = new Alarm();
  						alarm.setId(id);
  						alarm.setAlarmLevel(alarmLevel);
			            alarm.setTitle(rs.getString("TITLE"));
			            alarm.setSource(rs.getString("SRC"));
			            alarm.setStatus(status);
			            alarm.setDescription(rs.getString("DESCRIPTION"));
			            alarm.setOccuredTime(rs.getDate("OCCUREDTIME"));
			            Date processedTime = rs.getDate("PROCESSEDTIME");
			            if (processedTime != null)
			            {
			            	alarm.setProcessedTime(processedTime);
			            }
			            alarm.setErrorCode(rs.getInt("ERRORCODE"));
			            alarm.setTaskID(rs.getInt("TASKID"));
			            alarm.setSentTimes(sentTimes);
  					}
  					else
  					{
  						String update = "UPDATE UTL_DATA_ALARM SET STATUS=-2 WHERE ID=" + id;
  						try
  						{
  							CommonDB.executeUpdate(update);
  						}
  						catch (SQLException e)
  						{
  							log.error("更改告警状态为-2错误！" + update, e);
  						}
  					}
  					if (alarm == null)
  						continue;
  					AlarmMgr.this.alarmQ.put(alarm);
  					log.debug("构建1条告警！");
  				}
  			}
  		}

  		public void run()
  		{
  			log.info("告警扫描器开始扫描！");
  			while (isRunning())
  			{
  				Connection conn = null;
  				PreparedStatement stm = null;
  				ResultSet rs = null;
  				try
  				{
  					conn = CommonDB.getConnection();
  					stm = conn.prepareStatement("select * from UTL_DATA_ALARM where status=0 or status=-1 order by alarmlevel desc,occuredtime asc");
  					rs = stm.executeQuery();
  					builderAlarm(rs);
  				}
  				catch (Exception e)
  				{
  					log.error("告警扫描器发生错误！", e);
  				}
  				finally
  				{
  					CommonDB.close(rs, stm, conn);
  				}
  				try
  				{
  					Thread.sleep(5000L);
  				}
  				catch (InterruptedException e)
  				{
  					log.error("告警邮件扫描器睡眠被打断！", e);
  				}
  			}
  		}
  	}

  	class Sender
  		implements Runnable
  	{
  		private String updateSql = "UPDATE UTL_DATA_ALARM SET STATUS=%s,SENTTIMES=SENTTIMES+1,PROCESSEDTIME=to_date('%s','YYYY-MM-DD HH24:MI:SS') WHERE ID =%s";
  		private boolean runFlag = true;

  		Sender() {
  		}
  		public synchronized boolean isRunning() { return this.runFlag;
  		}

  		public synchronized void shutdown()
  		{
  			this.runFlag = false;
  		}

  		public void run()
    	{
  			log.info("告警发送器开始运行！");
  			while (isRunning())
  			{
  				String sql = null;
  				try
  				{
  					while (AlarmMgr.this.alarmSender == null) {
  						Thread.sleep(1000L);
  					}
  					Alarm alarm = (Alarm)AlarmMgr.this.alarmQ.get();
  					if (alarm == null) {
  						continue;
  					}
  					byte result = AlarmMgr.this.alarmSender.send(alarm);

  					Date now = new Date();

  					if (result == 0)
  					{
  						log.debug("消息发送成功！");
  						sql = String.format(this.updateSql, new Object[] { Integer.valueOf(1), Util.getDateString(now), Long.valueOf(alarm.getId()) });
  					}
  					else
  					{
  						log.debug("消息发送失败！");
  						sql = String.format(this.updateSql, new Object[] { Integer.valueOf(-1), Util.getDateString(now), Long.valueOf(alarm.getId()) });
  					}
  					CommonDB.executeUpdate(sql);
  				}
  				catch (Exception e)
  				{
  					log.error("更改告警状态错误！" + sql, e);
  				}
  			}
    	}
 	}
}