package com.turk.util;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

import com.turk.config.ConstDef;
import com.turk.config.SystemConfig;

public class Parsecmd
{
	private HashMap<String, String> movefilemap = new HashMap();
	private ArrayList<String> filelist;
	private boolean zipflags;
	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public void addfile(String sourfile, String newPath)
	{
		this.movefilemap.put(sourfile, newPath);
	}
	
	public static void main(String[] args)
	{
		BalySqlloadThread thread = new BalySqlloadThread();
		try
		{
			thread.runcmd("G:\\data\\test.bat");
		}
		catch (Exception e1)
		{
	      e1.printStackTrace();
		}
	}

	public void comitmovefiles()
	{
		if (this.movefilemap.size() < 1)
			return;
		String[] keys = (String[])this.movefilemap.keySet().toArray(new String[0]);
		this.filelist = new ArrayList();
		for (int i = 0; i < keys.length; i++)
		{
			try
			{
				this.zipflags = true;//(SystemConfig.getInstance().getMRZipFlag() == 1);
				movefile(keys[i], (String)this.movefilemap.get(keys[i]), this.zipflags);
			}
			catch (Exception e)
			{
				this.filelist.add(keys[i]);
				e.printStackTrace();
			}
			File f = new File(keys[i]);
			this.filelist.add((String)this.movefilemap.get(keys[i]) + File.separator + 
					f.getName());
		}
	}

	public static boolean movefile(String sourfile, String newPath)
	{
		return movefile(sourfile, newPath, false);
	}

	public static boolean movefile(String sourfile, String newPath, boolean zflag)
	{
		try
		{
			String os = System.getProperty("os.name").toLowerCase();
			String cmd = null;
			Process ldr = null;
			int nSucceed = 0;
			if (-1 != os.indexOf("windows"))
			{
				cmd = "cmd /c move " + sourfile + " " + newPath;
			}
			else
			{
				if (zflag)
				{
					cmd = "gzip " + sourfile;

					Thread.sleep(5000L);
					ldr = Runtime.getRuntime().exec(cmd);
					try
					{
						ldr.waitFor();
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
					catch (Exception e)
					{
						e.printStackTrace();
					}
					log.debug("zflag1=" + zflag + ";" + cmd + ";exitvalue=" + 
							ldr.exitValue());
					if (ldr.exitValue() == 0)
						sourfile = sourfile + ".gz";
				}
				cmd = "mv " + sourfile + " " + newPath;
				log.debug(cmd);
			}
			
			ldr = Runtime.getRuntime().exec(cmd);
			try
			{
				nSucceed = ldr.waitFor();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}

			if (nSucceed == 0)
			{
				File oldFile = new File(sourfile);

				File fnewpath = new File(newPath);

				if (!fnewpath.exists()) {
					fnewpath.mkdirs();
				}

				String ackName = newPath + File.separator + oldFile.getName() + ".ack";
				oldFile = null;
				File ackFile = new File(ackName);
				ackFile.createNewFile();

				log.debug("mv " + sourfile + " to " + newPath + " success");
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return false;
		}
		return true;
	}

	public static boolean ExecShellCmdByFtp(String strCmdList, Timestamp timestamp)
	{
		boolean bSuccesed = true;
		try
		{
			int sucess = 1;
			Runtime runtime = Runtime.getRuntime();
			Process ldr = null;
			String[] strCmd = strCmdList.split(";");
			for (int i = 0; i < strCmd.length; i++)
			{
				if ((strCmd[i] == null) || (strCmd[i].equals("")))
					continue;
				String strSql = ConstDef.ParseFilePath(strCmd[i], timestamp);
				ldr = runtime.exec(strSql);
				try
				{
					sucess = ldr.waitFor();
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
					bSuccesed = false;
				}
				if (sucess != 0) {
					bSuccesed = false;
				}
			}
		}
		catch (Exception Err)
		{
			Err.printStackTrace();
			bSuccesed = false;
		}
		return bSuccesed;
	}

	public static boolean ExecShellCmdByFtp1(String strCmdList, Timestamp timestamp)
	{
		boolean bSuccesed = true;
		BalySqlloadThread thread = new BalySqlloadThread();
		try
		{
			int sucess = 1;
			
			String[] strCmd = strCmdList.split(";");
			for (int i = 0; i < strCmd.length; i++)
			{
				if ((strCmd[i] == null) || (strCmd[i].equals("")))
					continue;
				String strSql = ConstDef.ParseFilePath(strCmd[i], timestamp);
				sucess = thread.runcmd(strSql);
				if (sucess != 0) {
					bSuccesed = false;
				}
			}

		}
		catch (Exception Err)
		{
			Err.printStackTrace();
			bSuccesed = false;
		}
		return bSuccesed;
	}

	public ArrayList<String> getFilelist()
	{
		return this.filelist;
	}

	public void setFilelist(ArrayList<String> filelist)
	{
		this.filelist = filelist;
	}
	
	public boolean isZipflags()
	{
  	  	return this.zipflags;
	}

	public void setZipflags(boolean zipflags)
	{
		this.zipflags = zipflags;
	}
}