package com.turk.console;

import com.turk.framework.DataLifecycleMgr;
import com.turk.framework.ScanThread;

import com.turk.Config.SystemConfig;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.turk.alarm.AlarmMgr;
import com.turk.datalog.DataLogMgr;
import com.turk.task.CollectObjInfo;
import com.turk.task.RegatherObjInfo;
import com.turk.task.TaskMgr;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.Task;
import com.turk.util.Util;

public class CmdHandler
{
	private ConsolePrinter printer = null;
	static final char N_CHAR = '\n';
	static final char R_CHAR = '\r';
	static final char B_CHAR = '\b';

	public CmdHandler(ConsolePrinter printer)
	{
		this.printer = printer;
	}

	public void list()
	{
		List<CollectObjInfo> lst = TaskMgr.getInstance().list();
		if (lst.size() == 0)
		{
			this.printer.println("暂无运行任务");
			return;
		}

		long now = System.currentTimeMillis();
		for (CollectObjInfo obj : lst)
		{
			int taskID = obj.getTaskID();
			String des = obj.getDescribe();
			Timestamp dataTime = obj.getLastCollectTime();

			String flag = "";
			if ((obj instanceof RegatherObjInfo))
				flag = "-" + String.valueOf(obj.getKeyID() - 10000000);
			String cost = "";
			long fast = now - obj.startTime.getTime();

			if (fast < 60000L) {
				cost = Math.round((float)(fast / 1000L)) + " 秒";
			}
			else
			{
				cost = Math.round((float)(fast / 60000L)) + " 分钟";
			}
			this.printer.println(taskID + flag + "   " + dataTime + "   " + des + 
					"  " + cost);
		}
		this.printer.println("总计： " + lst.size() + " 个");
	}

	String getInput(InputStream in)
    	throws IOException
    {
		return getInput(in, true);
    }

	String getInput(InputStream in, boolean echo)
		throws IOException
    {
		try
		{
			Thread.sleep(200L);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}

		StringBuilder buffer = new StringBuilder();

		int i = -1;

		while (((i = in.read()) != 13) && (i > -1))
		{
			if (!echo)
			{
				this.printer.maskChar();
			}

			if ((i != 8) && (i != 10))
			{
				buffer.append((char)i);
			}
			else if ((buffer.length() > 0) && 
					(buffer.charAt(buffer.length() - 1) != '\n'))
			{
				this.printer.backspace();
				buffer.deleteCharAt(buffer.length() - 1);
			}
			else if (i != 10)
			{
				this.printer.printNull();
			}
			else
			{
				if (buffer.length() != 0)
					continue;
				buffer.append('\n');
			}
		}

		if (buffer.length() == 0) 
			return "exit";

		String str = buffer.toString().trim();
		return str;
    }

	public void kill(String cmd, InputStream in)
	{
		if (cmd.length() <= 4)
		{
			this.printer.println("kill语法错误,缺少任务编号. 输入help/?获取命令帮助");
			return;
		}

		String[] strs = cmd.split(" ");
		if (strs.length != 2)
		{
			this.printer.println("kill语法错误. 输入help/?获取命令帮助");
			return;
		}
		if (!strs[0].equalsIgnoreCase("kill"))
		{
			this.printer.printUnSupportCmd(cmd);
		}

		String strTaskID = strs[1];
		int taskID = -1;
		try
		{
			taskID = Integer.parseInt(strTaskID);
		}
		catch (NumberFormatException localNumberFormatException)
		{
		}
		if (taskID == -1)
		{
			this.printer.println("kill语法错误,任务编号输入有误. 输入help/?获取命令帮助");
			return;
		}

		CollectObjInfo obj = TaskMgr.getInstance().getObjByID(taskID);
		if (obj == null)
		{
			obj = TaskMgr.getInstance().getObjByID(taskID + 10000000);
			if (obj == null)
			{
				this.printer.println("指定的任务编号当前不在运行状态或者任务编号不存在");
				return;
			}

		}

		String des = obj.getDescribe();
		Timestamp dataTime = obj.getLastCollectTime();
		this.printer.print("是否要杀死任务(" + taskID + ", " + dataTime + ", " + des + 
		" )?   [y|n]  ");
		String strLine = null;
		try
		{
			strLine = getInput(in);
		}
		catch (IOException e)
    	{
			this.printer.println("操作失败,内部错误:" + e.getMessage());
			return;
    	}
		strLine = strLine == null ? "" : strLine;

		if ((strLine.equalsIgnoreCase("n")) || (strLine.equalsIgnoreCase("no"))) {
			return;
		}
		if ((!strLine.equalsIgnoreCase("n")) && (!strLine.equalsIgnoreCase("no")) && 
				(!strLine.equalsIgnoreCase("y")) && 
				(!strLine.equalsIgnoreCase("yes")))
		{
			this.printer.println("非法输入,放弃操作.");
			return;
		}

		Task thrd = obj.getCollectThread();
		if (thrd != null)
		{
			//thrd.interrupt();
			thrd = null;
			TaskMgr.getInstance().delActiveTask(taskID, obj instanceof RegatherObjInfo);
		}
	}

	public void stop(String cmd, InputStream in)
	{
		if ((!cmd.equalsIgnoreCase("stop")) && (!cmd.equalsIgnoreCase("stop -i")))
		{
			this.printer.println("stop语法错误. 输入help/?获取命令帮助");
			return;
		}

		int taskCount = TaskMgr.getInstance().size();

		this.printer.print("当前有" + taskCount + "个任务在运行,是否要立即退出? [y|n]  ");
		String strLine = null;
		try
		{
			strLine = getInput(in);
		}
		catch (IOException e)
		{
			this.printer.println("操作失败,内部错误:" + e.getMessage());
			return;
		}
		strLine = strLine == null ? "" : strLine;

		if ((strLine.equalsIgnoreCase("n")) || (strLine.equalsIgnoreCase("no"))) {
			return;
		}
		if ((!strLine.equalsIgnoreCase("n")) && (!strLine.equalsIgnoreCase("no")) && 
				(!strLine.equalsIgnoreCase("y")) && 
				(!strLine.equalsIgnoreCase("yes")))
		{
			this.printer.println("非法输入,放弃停止采集系统.");
			return;
		}

		if (cmd.equalsIgnoreCase("stop"))
		{
			awaitStop();
		}
		else if (cmd.equalsIgnoreCase("stop -i"))
		{
			if (taskCount == 0)
				System.exit(0);
			else
				System.exit(-1);
		}
	}

	private void awaitStop()
	{
		ScanThread scanThread = ScanThread.getInstance();
		scanThread.setEndAction(new ScanThread.ScanEndAction()
		{
			public void actionPerformed(TaskMgr taskMgr)
			{
				List<CollectObjInfo> tasks = taskMgr.list();
				for (CollectObjInfo task : tasks)
				{
					String taskType = (task instanceof RegatherObjInfo) ? "补采任务" : "采集任务";
					int id = task.getKeyID();
					CmdHandler.this.printer.print("正在等待" + taskType + "(id:" + id + ")结束");
					CmdHandler.this.printer.println("......已结束");
				}

				DataLifecycleMgr.getInstance().stop();
				AlarmMgr.getInstance().shutdown();
				DataLogMgr.getInstance().commit();
				CommonDB.closeDbConnection();
				DbPool.close();

				Console.getInstance().stop();

				System.exit(0);
			}
		});
		scanThread.stopScan();
	}

	public void error()
	{
		String stdErrFile = "." + File.separator + "log" + File.separator + 
			"error.log";
		File fError = new File(stdErrFile);
		if ((fError.exists()) && (fError.isFile()))
		{
			BufferedReader br = null;
     		try
     		{
     			br = new BufferedReader(new InputStreamReader(new FileInputStream(fError)));
     			String strLine = null;
     			while ((strLine = br.readLine()) != null)
     			{
     				this.printer.println(strLine);
     			}
     		}
     		catch (FileNotFoundException e)
     		{
     			this.printer.println("标准错误端文件 " + stdErrFile + " 不存在");

     			if (br == null) 
     				return;
     			try {
     				br.close();
     			}
     			catch (IOException localIOException1)
     			{
     			}
     		}
     		catch (IOException e)
     		{
     			this.printer.println("获取标准错误端信息时异常,原因: " + e.getMessage());

     			if (br == null) 
     				return;
     			try {
     				br.close();
     			}
     			catch (IOException localIOException2)
     			{
     			}
     		}
     		finally
     		{
     			if (br != null)
     			{
     				try
     				{
     					br.close(); 
     				} 
     				catch (IOException localIOException3) 
     				{
     				}
     			}
     		}
     		try { 
     			br.close();
     		}
     		catch (IOException localIOException4)
     		{
     		}
		}
		else
		{
			this.printer.println("标准错误端文件 " + stdErrFile + " 不存在");
		}
	}

	public void thread(String cmd)
	{
		if ((!cmd.equalsIgnoreCase("thread")) && 
				(!cmd.equalsIgnoreCase("thread -c")))
		{
			this.printer.println("thread语法错误. 输入help/?获取命令帮助");
			return;
		}

		if (!cmd.equalsIgnoreCase("thread"))
		{
			if (cmd.equalsIgnoreCase("thread -c"))
			{
				this.printer.println("active thread count: " + Thread.activeCount());
			}
		}
	}

	/**
	 * 操作系统
	 */
	public void os()
	{
		Properties props = System.getProperties();
		String osName = props.getProperty("os.name");
		String osArch = props.getProperty("os.arch");
		String osVersion = props.getProperty("os.version");

		this.printer.println(osName + "  " + osArch + "  " + osVersion);
	}

	/**
	 * JAVA环境
	 */
	public void jvm()
	{
		float maxMemory = (float)(Runtime.getRuntime().maxMemory() / 1048576L);
		float totalMemory = (float)(Runtime.getRuntime().totalMemory() / 1048576L);
		float freeMemory = (float)(Runtime.getRuntime().freeMemory() / 1048576L);
		float usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;

		this.printer.println("jvm memory usage: ");
		this.printer.println("已使用: " + usedMemory + "M  剩余: " + freeMemory + 
				"M  最大内存: " + maxMemory + "M");
	}

	public void date()
	{
		this.printer.println(Util.getDateString(new Date()));
	}

 	public void host()
 	{
 		this.printer.println(Util.getHostName());
 	}

 	public void sys()
 	{
 		Date sDate = null;
    	try
    	{
    		sDate = (Date)Class.forName("framework.IGP").getDeclaredField("SYS_START_TIME").get(null);
    	}
    	catch (Exception e)
    	{
    		this.printer.println("错误,原因:" + e.getMessage());
    	}
    	String sysStartTime = Util.getDateString(sDate);
    	long fast = System.currentTimeMillis() - sDate.getTime();
    	String cost = "";

    	if (fast < 3600000L)
    	{
    		cost = Math.round((float)(fast / 60000L)) + " 分钟";
    	}
    	else
    	{	
    		cost = Math.round((float)(fast / 3600000L)) + " 小时";
    	}
    	this.printer.println("系统启动时间: " + sysStartTime + "  已运行: " + cost);
 	}

 	/**
 	 * 磁盘使用情况
 	 */
 	public void disk()
 	{
 		try
 		{
 			File[] roots = File.listRoots();
 			for (File f : roots)
 			{
 				float total = (float)f.getTotalSpace();
 				if (total == 0.0F)
 					continue;
 				float remain = (float)f.getFreeSpace();
 				int u = Math.round(remain / total * 100.0F);

 				this.printer.println(f.getPath() + "  " + 
 						remain / 1.073742E+009F + "GB可用   共 " + 
 						total / 1.073742E+009F + "GB   剩余: " + u + "%");
 			}
 		}
 		catch (Exception localException)
 		{
 		}
 	}

 	/**
 	 * 软件版本信息
 	 */
 	public void ver()
 	{
 		String edition = SystemConfig.getInstance().getEdition();
 		String releaseTime = SystemConfig.getInstance().getReleaseTime();
 		this.printer.println(edition + "  " + releaseTime);
 	}
}