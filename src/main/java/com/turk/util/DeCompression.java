package com.turk.util;



import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.templet.TempletBase;

/**
 * Ω‚—π∑Ω∑®
 * @author Administrator
 *
 */
public class DeCompression
{
	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public static ArrayList<String> decompress(int nTaskID, TempletBase base, String strFile, Timestamp timestamp, int nPeriod) throws Exception
	{
		if (Util.isWindows())
		{
			return decompressWin(nTaskID, base, strFile, timestamp, nPeriod);
		}

		return decompressUnix(nTaskID, base, strFile, timestamp, nPeriod);
	}

	private static ArrayList<String> decompressWin(int nTaskID, TempletBase base, String strFile, Timestamp timestamp, int nPeriod)
    	throws Exception
    {
		ArrayList filelist = new ArrayList();
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath() + 
			File.separatorChar + String.valueOf(nTaskID);

		log.debug("Decompress start:" + new Date());
		BufferedWriter bw = new BufferedWriter(
				new FileWriter(strCurrentPath + 
    		  File.separatorChar + "temp.bat", false));
		String strFolderName = "";
		SimpleDateFormat formatter = null;
		switch (nPeriod)
		{
			case 1:
				formatter = new SimpleDateFormat("yyyyMMdd");
				break;
			case 2:
				formatter = new SimpleDateFormat("yyyyMMdd");
				break;
			case 3:
				formatter = new SimpleDateFormat("yyyyMMddHH");
				break;
			case 4:
				formatter = new SimpleDateFormat("yyyyMMddHHmm");
				break;
			default:
				formatter = new SimpleDateFormat("yyyyMMddHHmm");
		}

		strFolderName = formatter.format(timestamp);
		bw.write("md " + strCurrentPath + File.separatorChar + strFolderName + 
			"\r\n");
		File dir = new File(strCurrentPath + File.separatorChar + strFolderName);

		String strWinrarPath = SystemConfig.getInstance().getWinrarPath();
		int nIndex = strWinrarPath.indexOf(":");
		if (nIndex >= 0)
			bw.write(strWinrarPath.substring(0, nIndex) + ":\r\n");
		bw.write("cd " + strWinrarPath + "\r\n");

		bw.write("winrar e " + strFile + " " + strCurrentPath + 
				File.separatorChar + strFolderName + " -y -ibck");
		bw.close();

		int nSucceed = 0;
		Process ldr = null;
		try
		{
			ldr = Runtime.getRuntime().exec(strCurrentPath + File.separatorChar + 
				"temp.bat");
			StreamGobbler errorGobbler = new StreamGobbler(ldr.getErrorStream(), "Error");
			StreamGobbler outputGobbler = new StreamGobbler(ldr.getInputStream(), "Output");
			errorGobbler.start();
			outputGobbler.start();
			
			log.debug("waitFor decompress");
			ldr.waitFor();
			
			//if (this.time == -1) {
			//Thread.sleep(10000L);
			//}
			
			nSucceed = ldr.exitValue();
		}
		catch (InterruptedException e)
		{
			throw e;
		}
		finally
		{
			ldr.destroy();
		}

		if (nSucceed != 1)
		{
			File[] files = dir.listFiles();

			for (int i = 0; i < files.length; i++)
			{
				String FilePath = files[i].getAbsolutePath();
				filelist.add(FilePath);
			}

			File fTar = new File(strFile);
			fTar.delete();

			File fBat = new File(strCurrentPath + File.separatorChar + 
			"temp.bat");
			fBat.delete();
		}
		else
		{
			throw new Exception("decompress file error. file:" + strFile + " exit value:" + nSucceed);
		}

		log.debug("Decompress end:" + new Date());

		return filelist;
    }

	private static ArrayList<String> decompressUnix(int nTaskID, TempletBase base, String strFile, Timestamp timestamp, int nPeriod)
    	throws Exception
    {
		ArrayList fileList = null;
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath() + 
			File.separatorChar + nTaskID;

		String strFolderName = "";
		SimpleDateFormat sdf = null;
		switch (nPeriod)
		{
			case 1:
				sdf = new SimpleDateFormat("yyyyMMdd");
				break;
			case 2:
				sdf = new SimpleDateFormat("yyyyMMdd");
				break;
			case 3:
				sdf = new SimpleDateFormat("yyyyMMddHH");
				break;
			case 4:
				sdf = new SimpleDateFormat("yyyyMMddHHmm");
				break;
			default:
				sdf = new SimpleDateFormat("yyyyMMddHHmm");
		}

		strFolderName = sdf.format(timestamp);

		String strLogFile = strFile.substring(0, strFile.lastIndexOf('.'));

		File fFolder = new File(strCurrentPath + File.separatorChar + 
				strFolderName);
		if (!fFolder.exists())
		{
			if (!fFolder.mkdir()) throw new Exception("mkdir error");
		}

		String cmd = (strFile.endsWith(".zip") ? "unzip " : "gzip -d ") + 
			strFile + (
				strFile.endsWith(".zip") ? " -d " + fFolder.getAbsolutePath() : "");
		log.debug("cmd - " + cmd);
		int retCode = Util.execExternalCmd(cmd);

		if (retCode != 0)
		{
			log.error(cmd + " retCode=" + retCode);
			return null;
		}

		log.debug(cmd + " retCode=" + retCode);

		cmd = "mv " + strLogFile + " " + strCurrentPath + File.separatorChar + 
			strFolderName;
		retCode = 0;
		retCode = Util.execExternalCmd(cmd);
		
		if (retCode == 0)
		{
			fileList = new ArrayList();

			File dir = new File(fFolder.getAbsolutePath());
			log.debug("dir - " + dir.getAbsolutePath());
			File[] files = dir.listFiles();
      
			for (int i = 0; i < files.length; i++)
			{
				String FilePath = files[i].getAbsolutePath();
				fileList.add(FilePath);
			}

			File fTar = new File(strFile);
			fTar.delete();
		}

		log.debug(cmd + " retCode=" + retCode);

		return fileList;
    }
	
	
}

class StreamGobbler extends Thread
{
	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	InputStream is;
	String type;

	StreamGobbler(InputStream is, String type)
	{
		this.is = is;
		this.type = type;
	}

	@SuppressWarnings("unused")
	public void run()
	{
		try
		{
			InputStreamReader isr = new InputStreamReader(this.is);
			BufferedReader br = new BufferedReader(isr);

			String line = null;
			while ((line = br.readLine()) != null);
			if(line!=null)
				log.debug(line);
		}
		catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}