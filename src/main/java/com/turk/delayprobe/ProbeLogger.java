package com.turk.delayprobe;

import com.turk.Config.SystemConfig;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import com.turk.util.Util;

/**
 * 探针日志
 * @author Administrator
 *
 */
public class ProbeLogger
{
	private int taskId;
	private FileWriter writer;
	private File logDir;
	private File logFile;
	private static final String SEPARATOR = File.separator;
	private static final String LINE_SEP = System.getProperty("line.separator");
	public ProbeLogger(int taskId)
	{
		if (!SystemConfig.getInstance().isEnableProbeLog()) return;
		this.taskId = taskId;

		this.logDir = 
			new File("." + SEPARATOR + "log" + SEPARATOR + "delay_log" + 
					SEPARATOR + this.taskId);
		if (!this.logDir.exists())
		{
			this.logDir.mkdirs();
		}
		this.logFile = new File(this.logDir, "delay.log");
		if ((this.logFile.exists()) && (this.logFile.length() >= 10485760L))
		{
			File history = new File(this.logDir, "delay.log." + 
					Util.getDateString_yyyyMMddHHmmssSSS(new Date()) + 
			".history");
			this.logFile.renameTo(history);
			this.logFile.delete();
			try
			{
				this.logFile.createNewFile();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}

		try
		{
			if (!this.logFile.exists())
			{
				this.logFile.createNewFile();
			}
			this.writer = new FileWriter(this.logFile, true);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
  	}

	public void println(String s)
	{
		if (!SystemConfig.getInstance().isEnableProbeLog()) return;
		if (this.writer != null)
		{
			try
			{
				this.writer.write("[" + Util.getDateString(new Date()) + "] " + s + 
						LINE_SEP);
				this.writer.flush();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}

	public void dispose()
	{
		if (!SystemConfig.getInstance().isEnableProbeLog()) return;
		if (this.writer != null)
		{
			try
			{
				this.writer.flush();
				this.writer.close();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
  	  	}
	}

	public static void main(String[] args)
	{
		ProbeLogger log = new ProbeLogger(12);
		log.println("asdf有人");
		log.dispose();
	}
}