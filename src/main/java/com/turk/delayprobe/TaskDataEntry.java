package com.turk.delayprobe;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.*;

import javax.servlet.jsp.jstl.sql.Result;

import org.apache.commons.net.ftp.*;
import org.apache.log4j.Logger;

import com.turk.config.ConstDef;
import com.turk.config.SystemConfig;
import com.turk.task.CollectObjInfo;
import com.turk.templet.*;
import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class TaskDataEntry
{
	private CollectObjInfo taskInfo;
	private TaskDataEntry pre;
	private int eqCount;
	private int probeCount;
	private boolean isNoError;
	private List<DataEntry> entrys = new ArrayList<DataEntry>();
	private ProbeLogger probeLogger;
	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	public TaskDataEntry(CollectObjInfo taskInfo)
	{
		this.taskInfo = taskInfo;
		this.isNoError = init();
	}

	public void addEntry(DataEntry de)
	{
		if ((de != null) && (!this.entrys.contains(de)))
		{
			this.entrys.add(de);
		}
  	}

	public List<DataEntry> getAll()
	{
		return this.entrys;
	}

	public ProbeLogger getProbeLogger()
	{
		return this.probeLogger;
  	}

	public void setProbeLogger(ProbeLogger probeLogger)
	{
		this.probeLogger = probeLogger; 
	} 

	private Connection getConnection(CollectObjInfo task, int iSleepTime, byte maxTryTimes)
	{
	    if (task == null) {
	    	return null;
	    }
	    Connection conn = null;

	    conn = CommonDB.getConnection(task.getDBDriver(), task.getDBUrl(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

	    if (conn == null)
	    {
	    	String strLog = "Task-" + task.getTaskID();

	    	logger.error(strLog + ": 获取对方数据库连接失败,尝试重连 ... ");

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
	    			logger.error(strLog + ": 尝试数据库重连失败 (" + tryTimes + ") ... ");
	    		}
	    		sleepTime += sleepTime * 2;
	    	}

	    	if (conn == null)
	    	{
	    		logger.error(strLog + ": 多次获取对方数据库连接失败.");
	    	}
	    	else
	    	{
	    		logger.info(strLog + ": 数据库重连成功(" + tryTimes + ").");
	    	}
	    }

	    return conn;
	}
	
	private boolean init() 
   	{ 
		Connection con = null;
		try
		{
		if (taskInfo.getCollectType() == 6 || taskInfo.getCollectType() == 60)
		{
			int parseTempletId = taskInfo.getParseTmpID();
			Result rs = CommonDB.queryForResult((new StringBuilder("select tempfilename from utl_conf_templet where tmpid=")).append(parseTempletId).toString());
			String fileName = rs.getRows()[0].get("tempfilename").toString();
			AbstractTempletBase p = ((AbstractTempletBase) (taskInfo.getCollectType() != 6 ? ((AbstractTempletBase) (new DBAutoTempletP2())) : ((AbstractTempletBase) (new DBAutoTempletP()))));
			p.parseTemp(fileName);
			Map<?, ?> tables = (Map<?, ?>)p.getClass().getDeclaredMethod("getTemplets", new Class[0]).invoke(p, new Object[0]);
			con = CommonDB.getConnection(taskInfo.getDBDriver(), taskInfo.getDBUrl(), taskInfo.getDevInfo().getHostUser(), taskInfo.getDevInfo().getHostPwd());
			if (con == null)
				con = getConnection(taskInfo, 100, (byte)3);
			if (con == null)
				throw new Exception("获取对方数据库连接失败");
			String selectStatement;
			String next;
			long size;
			for (Iterator<?> it = tables.keySet().iterator(); it.hasNext(); entrys.add(new DataEntry((new StringBuilder(String.valueOf(next))).append(" [实际采集所用语句:").append(selectStatement).append("]").toString(), size)))
			{
				selectStatement = null;
				next = (String)it.next();
				Object sql = tables.get(next).getClass().getDeclaredMethod("getSql", new Class[0]).invoke(tables.get(next), new Object[0]);
				sql = sql != null ? ((Object) (sql.toString())) : "";
				Object condition = tables.get(next).getClass().getDeclaredMethod("getCondition", new Class[0]).invoke(tables.get(next), new Object[0]);
				condition = condition != null ? ((Object) (condition.toString())) : "";
				if (Util.isNotNull(sql.toString()))
					selectStatement = ConstDef.ParseFilePathForDB(sql.toString(), taskInfo.getLastCollectTime());
				else
				if (Util.isNotNull(condition.toString()))
				{
					String tmp = ConstDef.ParseFilePathForDB(next, taskInfo.getLastCollectTime());
					selectStatement = ConstDef.ParseFilePathForDB((new StringBuilder("select * from ")).append(tmp).append(" where ").append(condition.toString()).toString(), taskInfo.getLastCollectTime());
				} else
				{
					String tmp = ConstDef.ParseFilePathForDB(next, taskInfo.getLastCollectTime());
					selectStatement = ConstDef.ParseFilePathForDB((new StringBuilder("select * from ")).append(tmp).toString(), taskInfo.getLastCollectTime());
				}
				size = 0L;
				try
				{
					if (CommonDB.tableExists(con, next, taskInfo.getTaskID()))
					{
						size = CommonDB.getRowCount(con, selectStatement);
						Thread.sleep(50L);
					} else
					{
						size = -1L;
					}
				}
				catch (Exception e)
				{
					throw e;
				}
			}

		} 
		else if (taskInfo.getCollectType() == 3 && SystemConfig.getInstance().isProbeFTP())
		{
			String encode = taskInfo.getDevInfo().getEncode();
			String collectPaths[] = taskInfo.getCollectPath().split(";");
			FTPClient ftp = new FTPClient();
			ftp.connect(taskInfo.getDevInfo().getIP(), taskInfo.getDevPort());
			ftp.login(taskInfo.getDevInfo().getHostUser(), taskInfo.getDevInfo().getHostPwd());
			ftp.enterLocalPassiveMode();
			setFTPClientConfig(ftp);
			Set<String> tmp = new HashSet<String>();
			String as[];
			int k = (as = collectPaths).length;
			for (int i = 0; i < k; i++)
			{
				String path = as[i];
				if (!Util.isNull(path))
				{
					List<String> list = Util.listFTPDirs(path, taskInfo.getDevInfo().getIP(), taskInfo.getDevPort(), taskInfo.getDevInfo().getHostUser(),
							taskInfo.getDevInfo().getHostPwd(), encode, false);
					tmp.addAll(list);
				}
			}

			collectPaths = (String[])tmp.toArray(new String[0]);
			k = (as = collectPaths).length;
			for (int j = 0; j < k; j++)
			{
				String path = as[j];
				String fileName = ConstDef.ParseFilePath(path, taskInfo.getLastCollectTime());
				String o = fileName;
				fileName = Util.isNotNull(encode) ? new String(fileName.getBytes(encode), "iso_8859_1") : fileName;
				FTPFile fs[] = ftp.listFiles(fileName);
				if (fs.length == 0)
				{
					entrys.add(new DataEntry(o, -1L));
				} 
				else
				{
					FTPFile aftpfile[];
					int i1 = (aftpfile = fs).length;
					for (int l = 0; l < i1; l++)
					{
						FTPFile f = aftpfile[l];
						if (f != null)
							if (o.lastIndexOf("/") >= 0)
							{
								String str = o.substring(0, fileName.lastIndexOf("/") + 1);
								entrys.add(new DataEntry((new StringBuilder(String.valueOf(str))).append(f.getName()).toString(), f.getSize()));
							} else
							{
								entrys.add(new DataEntry(Util.isNotNull(encode) ? new String(f.getName().getBytes("iso_8859_1"), encode) : f.getName(), f.getSize()));
							}
					}

				}
			}

		ftp.disconnect();
		} 
		else
		{
			throw new Exception((new StringBuilder("不支持的采集类型:")).append(taskInfo.getCollectType()).toString());
		}
	}
	catch (Exception e)
	{
		logger.error("获取采集目标时异常", e);
	}
	if (con != null)
	{
		try
		{
			con.close();
		}
		catch (Exception exception1)
		{ 
			return false;
		}
	}
	
		if (con != null)
		{
			try
			{
				con.close();
			}
			catch 
			(Exception exception2) 
			{ }
		}
		if (con != null)
		{
			try
			{
				con.close();
			}
			catch (Exception exception3) 
			{ }
		}
		
		return true;
	} 
	
	public boolean isNoError() 
	{ 
		return this.isNoError;
	}

	public TaskDataEntry getPre()
	{
		return this.pre;
	}

	public void setPre(TaskDataEntry pre)
	{
		this.pre = pre;
	}

	public int getEqCount()
	{
		return this.eqCount;
	}

	public void setEqCount(int eqCount)
	{
		this.eqCount = eqCount;
	}

	public int getProbeCount()
	{
		return this.probeCount;
	}

	public void setProbeCount(int probeCount)
	{
		this.probeCount = probeCount;
	}

	private String formart(int max, int length)
	{
		if (length >= max) 
			return "    ";
		StringBuilder sb = new StringBuilder();
		int diff = max - length;
		for (int i = 0; i < diff; i++)
		{
			sb.append(" ");
		}
		sb.append("    ");
		return sb.toString();
	}
	
	private int maxLength(List<DataEntry> list)
	{
		int max = 0;
		for (DataEntry de : list)
		{
			if (de.getName().length() < max)
				continue;
			max = de.getName().length();
		}

		return max;
	}

	private void setFTPClientConfig(FTPClient ftp)
	{
		try
		{
			ftp.configure(new FTPClientConfig("UNIX"));
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("WINDOWS"));
			}
			else
			{
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("AS/400"));
			}
			else
			{
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("TYPE: L8"));
			}
			else
			{
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("MVS"));
			}
			else
			{
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("NETWARE"));
			}
			else
			{
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("OS/2"));
			}
			else
			{
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("OS/400"));
			}
			else
			{
				return;
			}
			if (!Util.isFileNotNull(ftp.listFiles("/*")))
			{
				ftp.configure(new FTPClientConfig("VMS"));
			}
			else
			{
				return;
			}
		}
		catch (Exception e)
		{
			logger.error("配置FTP客户端时异常", e);
		}
	}

	public boolean compare()
	{
		boolean b = false;
		int preCount = 0;
		int thisCount = 0;
		int max = maxLength(getAll());
		for (DataEntry de : getAll())
		{
			this.probeLogger.println("表名/文件名:" + de.getName() + 
					formart(max, de.getName().length()) + "记录数/尺寸:" + (
							de.getSize() == -1L ? "不存在" : Long.valueOf(de.getSize())));
			thisCount = (int)(thisCount + de.getSize());
		}
		if (this.pre != null)
		{
			for (DataEntry de : this.pre.getAll())
			{
				preCount = (int)(preCount + de.getSize());
			}
			if (preCount == thisCount)
			{
				this.eqCount += 1;
				b = true;
			}
			else
			{
				this.eqCount = 0;
			}
		}

		return b;
	}

	public static void main(String[] args)
		throws Exception
	{
		CollectObjInfo info = new CollectObjInfo(433);
		info.setLastCollectTime(new Timestamp(Util.getDate1("2010-07-10 00:00:00").getTime()));
		info.setCollectType(60);
		info.setParseTmpID(731);
		TaskDataEntry t = new TaskDataEntry(info);
		System.out.println(t.getAll());
	}
}