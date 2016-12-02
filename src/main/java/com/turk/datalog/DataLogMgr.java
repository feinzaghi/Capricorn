package com.turk.datalog;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.turk.config.SystemConfig;
import com.turk.util.DbPool;
import com.turk.util.ExternalCmd;
import com.turk.util.LogMgr;
import com.turk.util.Util;

/**
 * ������־����
 * @author Administrator
 *
 */
public class DataLogMgr
{
	private List<DataLogInfo> logs = new ArrayList<DataLogInfo>();

	private List<DataFtpLogInfo> ftplogs = new ArrayList<DataFtpLogInfo>();
	/**
	 * ������־�������
	 */
	private static final int INTERVAL = SystemConfig.getInstance().getDataLogInterval();

	private static final boolean IS_ENABLE = SystemConfig.getInstance().isEnableDataLog();
	private static DataLogMgr instance;
	private Logger logger = LogMgr.getInstance().getSystemLogger();

	public static synchronized DataLogMgr getInstance()
	{
		if (instance == null)
		{
			instance = new DataLogMgr();
		}
		return instance;
	}

	/**
	 * �����־
	 * @param info
	 */
	public void addLog(DataLogInfo info)
	{
		if (!IS_ENABLE) 
			return;
		if (info == null)
		{
			this.logger.error("Ҫ��ӵ���־Ϊnull");
			return;
		}
		synchronized (this.logs)
		{
			info.setLogTime(new Date());
			this.logs.add(info);
			if (this.logs.size() >= INTERVAL)
			{
				commit();
			}
		}
	}
	
	/**
	 * ��¼ftp�ɼ��ļ���־
	 * @param info
	 */
	public synchronized void addFtpLog(DataFtpLogInfo info)
	{
		
		if (info == null)
		{
			this.logger.error("Ҫ��ӵ���־Ϊnull");
			return;
		}
		Connection con = DbPool.getConn();
		Statement st = null;
		try
		{
			con.setAutoCommit(false);
			st = con.createStatement();
			
			String insert = "insert into utl_data_ftpfilelog " +
					"(task_id,collecttime,stamptime,filename,deviceid,is_cal,filesize) " +
					"values (%d,sysdate,%s,'%s',%d,0,%d)";

			insert = String.format(insert, new Object[] {  info.getTaskID(),
					"to_date('" + Util.getDateString(info.getStampTime()) + 
					"','yyyy-mm-dd hh24:mi:ss')",info.getFileName(),
					info.getDeviceID(),info.getFileSize() });
			st.addBatch(insert);
			
			st.executeBatch();
			con.commit();
			this.ftplogs.clear();
		}
		catch (Exception e)
		{
			this.logger.error("�ύ���ݿ���־ʱ�쳣��" + this.ftplogs.size() + "����־δ�ύ��", e);
			return;
		}
		finally
		{
			try
			{
				if (st != null)
				{
					st.close();
				}
				if (con != null)
				{
					con.close();
				}
			}
			catch (Exception localException2)
			{
			}
		}
		//synchronized (this.ftplogs)
		//{
		//	this.ftplogs.add(info);
		//}
	}

	public void commit()
	{
		if (!IS_ENABLE) return;
		synchronized (this.logs)
		{
			if (SystemConfig.getInstance().isSqlldrDataLog())
			{
				sqlldrCommit();
			}
			else
			{
				jdbcCommit();
			}
		}
	}

	private void sqlldrCommit()
	{
		try
		{
			String currenDate = Util.getDateString_yyyyMMddHHmmssSSS(new Date());
			File dir = new File(SystemConfig.getInstance().getCurrentPath() + 
					File.separator + "data_log");
			if (!dir.exists())
			{
				dir.mkdir();
			}
			File ctl = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + 
				".ctl");
			File txt = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + 
				".txt");
			File log = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + 
				".log");
			File bad = new File(dir.getAbsoluteFile(), "data_log_" + currenDate + 
				".bad");

			StringBuilder bufferCtl = new StringBuilder();
			bufferCtl.append("load data\nCHARACTERSET ").append(SystemConfig.getInstance().getSqlldrCharset()).append("\n");
			bufferCtl.append("infile '").append(txt.getAbsolutePath()).append("'");
			bufferCtl.append(" append into table UTL_DATA_LOG\n").append("FIELDS TERMINATED BY \";\"\n");
			bufferCtl.append("TRAILING NULLCOLS\n").append("(LOG_TIME DATE 'YYYY-MM-DD HH24:MI:SS',TASK_ID,TASK_DESCRIPTION,TASK_TYPE,");
			bufferCtl.append("TASK_STATUS,TASK_DETAIL CHAR(4000),TASK_EXCEPTION CHAR(4000),");
			bufferCtl.append("DATA_TIME DATE 'YYYY-MM-DD HH24:MI:SS',COST_TIME,TASK_RESULT)");
			PrintWriter pw = new PrintWriter(ctl);
			pw.print(bufferCtl);
			pw.flush();
			pw.close();

			pw = new PrintWriter(txt);
			for (DataLogInfo d : this.logs)
			{
				StringBuilder tmp = new StringBuilder();
				tmp.append(d.getLogTime() == null ? "" : Util.getDateString(d.getLogTime())).append(";");
				tmp.append(d.getTaskId()).append(";");
				tmp.append(handleStringField(d.getTaskDescription(), 254)).append(";");
				tmp.append(handleStringField(d.getTaskType(), 50)).append(";");
				tmp.append(handleStringField(d.getTaskStatus(), 50)).append(";");
				tmp.append(handleStringField(d.getTaskDetail(), 4000)).append(";");
				tmp.append(handleStringField(d.getTaskException(), 4000)).append(";");
				tmp.append(d.getDataTime() == null ? "" : Util.getDateString(d.getDataTime())).append(";");
				tmp.append(d.getCostTime()).append(";");
				tmp.append(handleStringField(d.getTaskResult(), 50));
				pw.println(tmp);
			}
			pw.flush();
			pw.close();

			String cmd = String.format("sqlldr userid=%s/%s@%s skip=0 control=%s bad=%s log=%s errors=9999", new Object[] { SystemConfig.getInstance().getDbUserName(), SystemConfig.getInstance().getDbPassword(), SystemConfig.getInstance().getDbService(), ctl.getAbsoluteFile(), bad.getAbsoluteFile(), log.getAbsoluteFile() });
			this.logger.debug("��ʼ�ύ���ݿ���־: " + 
					cmd.replace(SystemConfig.getInstance().getDbUserName(), "*").replace(SystemConfig.getInstance().getDbPassword(), "*"));
			int code = new ExternalCmd().execute(cmd);
			this.logger.debug("���ݿ���־�ύ��ϣ�������Ϊ: " + code);
			if ((SystemConfig.getInstance().isDelDataLogTmpFile()) && (code == 0))
			{
				ctl.delete();
		        log.delete();
		        bad.delete();
		        txt.delete();
			}
		}
		catch (Exception e)
		{
			this.logger.error("�ύ���ݿ���־ʱ�쳣", e);
			return;
		}
		finally
		{
			this.logs.clear();
		}
	}

	private void jdbcCommit()
	{
		Connection con = DbPool.getConn();
		Statement st = null;
		try
		{
			con.setAutoCommit(false);
			st = con.createStatement();
			for (DataLogInfo d : this.logs)
			{
				String insert = "insert into utl_data_log (log_time,task_id,task_description,task_type,task_status,task_detail,task_exception,data_time,cost_time,task_result) values (%s,%s,'%s','%s','%s','%s','%s',%s,%s,'%s')";

				insert = String.format(insert, new Object[] { "to_date('" + 
						Util.getDateString(d.getLogTime()) + 
						"','yyyy-mm-dd hh24:mi:ss')", Integer.valueOf(d.getTaskId()), handleStringField(d.getTaskDescription(), 255).replace("'", "''"), handleStringField(d.getTaskType(), 50), handleStringField(d.getTaskStatus(), 50), handleStringField(d.getTaskDetail(), 4000), handleStringField(d.getTaskException(), 4000).replace("'", "''"), "to_date('" + 
						Util.getDateString(d.getDataTime()) + 
						"','yyyy-mm-dd hh24:mi:ss')", Long.valueOf(d.getCostTime()), handleStringField(d.getTaskResult(), 50) });
				st.addBatch(insert);
			}
			st.executeBatch();
			con.commit();
			this.logs.clear();
		}
		catch (Exception e)
		{
			this.logger.error("�ύ���ݿ���־ʱ�쳣��" + this.logs.size() + "����־δ�ύ��", e);
			return;
		}
		finally
		{
			try
			{
				if (st != null)
				{
					st.close();
				}
				if (con != null)
				{
					con.close();
				}
			}
			catch (Exception localException2)
			{
			}
		}
	}
	
	/**
	 * FTP�ɼ���־�ύ
	 */
	public void FtpLogCommint()
	{
		Connection con = DbPool.getConn();
		Statement st = null;
		try
		{
			con.setAutoCommit(false);
			st = con.createStatement();
			for (DataFtpLogInfo d : this.ftplogs)
			{
				String insert = "insert into utl_data_ftpfilelog " +
						"(task_id,collecttime,stamptime,filename,deviceid,is_cal) " +
						"values (%d,sysdate,%s,'%s',%d,0)";

				insert = String.format(insert, new Object[] {  d.getTaskID(),
						"to_date('" + Util.getDateString(d.getStampTime()) + 
						"','yyyy-mm-dd hh24:mi:ss')",d.getFileName(),d.getDeviceID() });
				st.addBatch(insert);
			}
			st.executeBatch();
			con.commit();
			this.ftplogs.clear();
		}
		catch (Exception e)
		{
			this.logger.error("�ύ���ݿ���־ʱ�쳣��" + this.ftplogs.size() + "����־δ�ύ��", e);
			return;
		}
		finally
		{
			try
			{
				if (st != null)
				{
					st.close();
				}
				if (con != null)
				{
					con.close();
				}
			}
			catch (Exception localException2)
			{
			}
		}
	}

	private String handleStringField(String s, int maxSize)
	{
		byte[] bs = s.getBytes();
		if (bs.length > maxSize)
		{
			s = new String(bs, 0, maxSize);
		}	
		if (SystemConfig.getInstance().isSqlldrDataLog())
		{
			s = s.replaceAll("\n", " ").replaceAll("\r", " ").replaceAll(";", " ");
		}
		return s;
	}

	public static void main(String[] args) throws Exception
	{
		DataLogInfo info = new DataLogInfo(new Timestamp(Util.getDate1("2010-07-23 17:32:23").getTime()), 4476, "��'������", "��������", "����", "��;;;\n\r��һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ���'��һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ��־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ�²���һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ����־ģ�����һ��", "eeeeeee", new Timestamp(Util.getDate1("2010-07-10 00:00:00").getTime()), 654654321L, "sdf");

		getInstance().addLog(info);

		getInstance().commit();
	}
}