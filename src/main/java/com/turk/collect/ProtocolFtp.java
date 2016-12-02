package com.turk.collect;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import com.turk.task.CollectObjInfo;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class ProtocolFtp
{
	private FTPClient FTP;
	private String host;
	private int port;
	private String user;
	private String pwd;
	private int taskid;
	private String taskdate;
	private boolean forceexit;
	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public String toString()
	{
		return this.host + ":" + this.port + "@" + this.user + "/" + this.pwd;
	}

	public boolean Login(String strHost, int nPort, String strUser, String strPwd)
	{
		this.host = strHost;
		this.port = nPort;
		this.user = strUser;
		this.pwd = strPwd;

		boolean bOK = false;
		try
		{
			if (this.FTP == null)
			{
				this.FTP = new FTPClient();
			}
			else
			{
				try
				{
					this.FTP.disconnect();
				}
				catch (Exception localException1)
				{
				}
			}

			this.FTP.connect(strHost, nPort);

			int reply = this.FTP.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply))
			{
				this.FTP.disconnect();
				log.error("FTP server refused connection.");
				return false;
			}

			bOK = this.FTP.login(strUser, strPwd);
			if (bOK)
			{
				this.FTP = Util.setFTPClientConfig(this.FTP, this.host, this.port, this.user, this.pwd);
				this.FTP.enterLocalPassiveMode();
				this.FTP.setControlEncoding("GBK");
				this.FTP.setFileType(2);
				this.FTP.setDataTimeout(3600000);
				this.FTP.setDefaultTimeout(3600000);
			}
			else
			{
				log.error("FTP server Login Failure Code:" + this.FTP.getReplyCode());
			}
		}
		catch (SocketException se)
		{
			log.error("FTP login", se);
		}
		catch (Exception e)
		{
			log.error("FTP login", e);
		}

		return bOK;
	}

	public boolean login(CollectObjInfo task, int iSleepTime, byte maxTryTimes)
	{
		if (task == null) {
			return false;
		}
		boolean bOK = false;

		bOK = Login(task.getDevInfo().getIP(), task.getDevPort(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

		if (!bOK)
		{
			String strLog = task.getSysName();

			log.error(strLog + ": FTP登陆失败,尝试重新登陆 ... ");

			byte tryTimes = 0;
			int sleepTime = iSleepTime;
			while ((tryTimes < maxTryTimes) && (!ftpValidate()))
			{
				try
				{
					Thread.currentThread(); Thread.sleep(sleepTime);
				}
				catch (InterruptedException e)
				{
					break;
				}

				bOK = Login(task.getDevInfo().getIP(), task.getDevPort(), task.getDevInfo().getHostUser(), task.getDevInfo().getHostPwd());

				tryTimes = (byte)(tryTimes + 1);

				if (bOK)
					break;
				log.error(strLog + ": 尝试重新登陆FTP失败 (" + tryTimes + ") ... ");

				sleepTime += sleepTime * 2;
			}

			if (!bOK)
			{
				log.error(strLog + ": " + tryTimes + "次FTP登陆重试失败.");
			}
			else
			{
				log.info(strLog + ": FTP登陆重试成功(" + tryTimes + ").");
			}
		}

		return bOK;
	}

	public void Close()
	{
		try
		{
			if (this.FTP != null)
			{
				this.FTP.logout();
				this.FTP.disconnect();
			}
   		 }
		catch (Exception localException)
		{
		}
	}

	private boolean ftpValidate()
	{
		return (this.FTP != null) && (this.FTP.isConnected());
	}

	public boolean ReLogin()
	{
		int i = 1;
		boolean ret = false;

		if (ftpValidate()) 
			return true;

		do
		{
			if (i > 3) 
				return ret;

			log.debug(getTaskid() + ": 第" + i + "次ReLogin登陆ftp:" + this.host + "," + 
					this.user);
			try
			{
				Thread.sleep(1000 * i * 30);
			}
			catch (Exception e)
			{
				log.error(getTaskid() + ": ftp relogin failed. ", e);
			}

			ret = Login(this.host, this.port, this.user, this.pwd);
			if (ret) {
				break;
			}
			i++;
		}
		while (!ftpValidate());

		return ret;
	}

	public String[] downFile(String strFile, String strLocalPath, String encode)
		throws Exception
	{
		if (!checkConnection()) return null;

		String[] retFiles = (String[])null;
		try
		{
			if ((this.FTP != null) && (this.FTP.isConnected()))
			{
				this.FTP.setControlEncoding("GBK");

				String strPath = "";

				int nFind = strFile.lastIndexOf('\\');
				if (nFind != -1)
				{
					strPath = strFile.substring(0, nFind + 1);
				}
				else
				{
					nFind = strFile.lastIndexOf('/');
					if (nFind != -1) {
						strPath = strFile.substring(0, nFind + 1);
					}
				}
				String[] fileNames = (String[])null;
				try
				{
					fileNames = listNames(strFile, encode);
				}
				catch (Exception e)
				{
					throw e;
				}

				if ((fileNames == null) || (fileNames.length == 0))
				{
					for (int times = 0; times < 3; times++)
					{
						int delay = (times + 1) * 1500;
						log.error(getTaskid() + ": " + 
								this.FTP.getRemoteAddress().toString() + ":" + 
								strFile + " 不存在，开始重试，次数:" + (times + 1));
						Thread.sleep(delay);
						this.FTP.disconnect();
						this.FTP.connect(this.host, this.port);
						this.FTP.login(this.user, this.pwd);
						this.FTP = Util.setFTPClientConfig(this.FTP, this.host, this.port, this.user, this.pwd);
						this.FTP.setControlEncoding("GBK");
						this.FTP.setFileType(2);
						this.FTP.setDataTimeout(3600000);
						this.FTP.setDefaultTimeout(3600000);
						Thread.sleep(500L);
						fileNames = listNames(strFile, encode);
						if ((fileNames != null) && (fileNames.length > 0))
						{
							break;
						}
					}
					if ((fileNames == null) || (fileNames.length == 0))
					{
						log.error(getTaskid() + ": " + 
								this.FTP.getRemoteAddress().toString() + ":" + 
								strFile + " 不存在，已重试3次");
						return null;
					}
				}

				retFiles = new String[fileNames.length];

				Long beg = Long.valueOf(System.currentTimeMillis());

				for (int i = 0; i < fileNames.length; i++)
				{
					String strFileName = fileNames[i];
					nFind = strFileName.lastIndexOf('\\');
					if (nFind != -1)
					{
						strFileName = strFileName.substring(nFind + 1);
					}
					else
					{
						nFind = strFileName.lastIndexOf('/');
						if (nFind != -1) {
							strFileName = strFileName.substring(nFind + 1);
						}
					}
					File lpath = new File(strLocalPath);

					if (!lpath.exists()) {
						lpath.mkdir();
					}
					if (downloadOneFile(strFileName, strLocalPath, strPath, encode))
					{
						retFiles[i] = 
							(strLocalPath + File.separator + 
									strFileName);
					}
					else
					{
						if (ReLogin())
						{
							if (downloadOneFile(strFileName, strLocalPath, strPath, encode))
								continue;
							log.error(getTaskid() + ": 下载文件失败" + 
									strFileName);
							log.debug(getTaskid() + " 删掉本地文件: " + 
									fileNames[i]);
							File f = new File(strLocalPath + File.separator + 
									strFileName);
							f.delete();
							retFiles[i] = null;

							throw new Exception("下载文件失败.");
						}
						throw new Exception("下载文件失败后重新登陆失败.");
					}	
				}

				log.debug(getTaskid() + " : 数据时间=" + this.taskdate + 
						" 文件下载完成; 耗时:" + 
						(System.currentTimeMillis() - beg.longValue()) / 1000L);
			}
		}
		catch (Exception e)
		{
			log.error(getTaskid() + ": down file error :" + strFile + 
					" loacl file :" + strLocalPath, e);
			retFiles = (String[])null;
			throw e;
		}
		return retFiles;
	}

	private boolean checkConnection()
	{
		boolean bReturn = true;

		this.forceexit = false;

		if (!ftpValidate())
		{
			if (!ReLogin())
			{
				this.forceexit = true;
				bReturn = false;
			}
		}

		return bReturn;
	}

	private String[] listNames(String strFile, String encode) throws Exception
	{
		if (!checkConnection()) 
			return null;

		String[] names = (String[])null;
		try
		{
			names = this.FTP.listNames(new String(strFile.getBytes(Util.isNotNull(encode) ? encode : "GBK"), "iso-8859-1"));
			if ((names == null) || (names.length == 0))
			{
				FTPFile[] fs = this.FTP.listFiles(new String(strFile.getBytes(Util.isNotNull(encode) ? encode : "GBK"), "iso-8859-1"));
				List<String> tmp = new ArrayList<String>();
				for (FTPFile f : fs)
				{
					if (!f.isFile())
						continue;
					tmp.add(f.getName());
				}

				names = (String[])tmp.toArray(new String[0]);
			}
		}
		catch (Exception e)
		{
			log.error(getTaskid() + " : error when FTP list names. " + strFile, e);

			if (checkConnection())
			{
				try
				{
					this.FTP.disconnect();
					Thread.sleep(500L);
					this.FTP.connect(this.host, this.port);
					this.FTP.login(this.user, this.pwd);
					this.FTP = Util.setFTPClientConfig(this.FTP, this.host, this.port, this.user, this.pwd);
					this.FTP.enterLocalPassiveMode();
					this.FTP.setFileType(2);

					this.FTP.setDataTimeout(3600000);
					this.FTP.setDefaultTimeout(3600000);
					names = this.FTP.listNames(new String(strFile.getBytes("GBK"), "iso-8859-1"));
				}
				catch (Exception e1)
				{
					log.error(getTaskid() + " : 尝试再次listNames失败.", e1);
					throw e1;
				}
			}
			else
			{
				log.error(getTaskid() + " : listNames失败后尝试重连失败.");
				throw e;
			}
		}
		return names;
	}

	private boolean downloadOneFile(String fileName, String localPath, String path, String encode)
	{
		boolean result = false;
		long ftpFileLength = -1L;
		String fullName = path + fileName;
		String tmp = fullName;
		try
		{
			fullName = new String(fullName.getBytes(Util.isNull(encode) ? "gbk" : encode), "iso_8859_1");
		}
		catch (UnsupportedEncodingException e1)
		{
			e1.printStackTrace();
		}
		try
		{
			FTPFile[] fs = this.FTP.listFiles(fullName);
			if (fs.length == 0)
			{
				for (int times = 0; times < 3; times++)
				{
					int delay = (times + 1) * 1500;
					Thread.sleep(delay);
					this.FTP.disconnect();
					Thread.sleep(500L);
					this.FTP.connect(this.host, this.port);
					this.FTP.login(this.user, this.pwd);
					this.FTP = Util.setFTPClientConfig(this.FTP, this.host, this.port, this.user, this.pwd);
					this.FTP.enterLocalPassiveMode();
					this.FTP.setFileType(2);
					this.FTP.setDataTimeout(3600000);
					this.FTP.setDefaultTimeout(3600000);
					fs = this.FTP.listFiles(fullName);
					if (fs.length > 0)
						break;
				}
			}
			int delay;
			if (fs.length == 0)
			{
				for (int times = 0; times < 3; times++)
				{
					delay = (times + 1) * 1500;
					Thread.sleep(delay);
					this.FTP.disconnect();
					Thread.sleep(500L);
					this.FTP.connect(this.host, this.port);
					this.FTP.login(this.user, this.pwd);
					this.FTP = Util.setFTPClientConfig(this.FTP, this.host, this.port, this.user, this.pwd);
					this.FTP.setDataTimeout(3600000);
					this.FTP.setDefaultTimeout(3600000);
					fs = this.FTP.listFiles(fullName);
					if (fs.length > 0)
					{
						break;
					}
				}
			}

			if (fs.length > 0)
			{
				for (FTPFile f : fs)
				{
					if ((f.getName().equals(".")) || (f.getName().equals("..")))
						continue;
					ftpFileLength = f.getSize();
					log.debug(tmp + "在FTP上的文件大小为：" + ftpFileLength);
				}
			}

			if ((fs.length == 0) || (ftpFileLength == -1L))
				throw new Exception("未能在FTP上找到文件:" + 
						tmp);
		}
		catch (Exception ex)
		{
			log.error("ftp,fileLength=-1");

			File tdFile = null;
			try
			{
				File dir = new File(localPath);
				if (!dir.exists())
				{
					dir.mkdirs();
				}
				tdFile = new File(localPath, fileName + ".td_" + 
						Util.getDateString_yyyyMMddHH(Util.getDate1(this.taskdate)));
			}
			catch (ParseException e1)
			{
				e1.printStackTrace();
			}
			if (!tdFile.exists())
			{
				try
				{
					tdFile.createNewFile();
				}
				catch (IOException e)
				{
					log.error("临时文件创建失败：" + tdFile.getAbsoluteFile(), e);
					return false;
				}
			}
			else if (tdFile.length() >= ftpFileLength)
			{
				log.debug(getTaskid() + " 文件下载成功: " + fileName);
				result = true;
				return result; 
			} 
			FileOutputStream fos = null;

			MonitorThread monitor = null;
			InputStream in = null;
			File f;
			boolean b;
			try { 
				fos = new FileOutputStream(tdFile, true);
				log.debug(getTaskid() + ": 开始下载文件:" + path + fileName);

				monitor = new MonitorThread(3600, this.FTP, getTaskid(), Thread.currentThread());
				monitor.start();
				in = this.FTP.retrieveFileStream(new String((path + fileName).getBytes(Util.isNull(encode) ? "GBK" : encode), "iso-8859-1"));
				long tdLength = tdFile.length();
				in.skip(tdLength);
				byte[] bytes = new byte[1024];
				int c;
				while ((c = in.read(bytes)) != -1)
				{
					fos.write(bytes, 0, c);
				}
				result = (tdFile.length() >= ftpFileLength) || (ftpFileLength == -1L);

				if (result)
				{
					fos.flush();
					log.debug(getTaskid() + "-文件下载成功: " + fileName);
				}
				else
				{
					log.error(getTaskid() + "-文件未完整下载，文件大小不一致，" + tmp + ":" + 
							ftpFileLength + "," + tdFile.getAbsoluteFile() + ":" + 
							tdFile.length());
				}
			}
			catch (ClosedByInterruptException e)
			{
				log.error("任务 " + getTaskid() + " ftp时由于超时被MonitorThread中断.");
				result = false;
			}
			catch (Exception e)
			{
				log.error(getTaskid() + ": downloadOneFile: down file error :" + 
						fileName + " loacl file :" + localPath, e);
				result = false;
			}
			finally
			{
				if (in != null)
				{
					try
					{
						in.close();
						this.FTP.completePendingCommand();
					}
					catch (Exception localException3)
					{
					}
				}
				if (fos != null)
				{
					try
					{
						fos.close();
					}
					catch (IOException e)
					{
						fos = null;
					}
				}

				if (result)
				{
					f = new File(localPath, fileName);
					if (f.exists())
					{
						f.delete();
					}
					b = tdFile.renameTo(f);
					if (!b)
					{
						result = false;
						log.error("将" + tdFile.getAbsolutePath() + "重命名为" + 
								f.getAbsolutePath() + "时失败，" + 
								f.getAbsolutePath() + "被占用");
					}
					tdFile.delete();
				}

				if (monitor != null)
					monitor.interrupt();
			}
		}
		return result;
	}

	public boolean isForceexit()
	{
		return this.forceexit;
	}

	public void setForceexit(boolean forceexit)
	{
		this.forceexit = forceexit;
	}

	public int getTaskid()
	{
		return this.taskid;
	}

	public void setTaskid(int taskid)
	{
		this.taskid = taskid;
	}

	public String getTaskdate()
	{
		return this.taskdate;
	}	

	public void setTaskdate(String taskdate)
	{
		this.taskdate = taskdate;
	}

	public static void main(String[] args)
	{
	}

	class MonitorThread extends Thread
	{
		FTPClient ftpClient = null;
		int nSeconds = 0;
		int nTaskID = 0;
		Thread ftpThread = null;

		public MonitorThread(int nSeconds, FTPClient ftp, int nTaskID, Thread t)
		{
			this.ftpClient = ftp;
			this.nSeconds = nSeconds;
			this.nTaskID = nTaskID;
			this.ftpThread = t;
		}

		public void run()
		{
			if (this.nSeconds > 0)
			{
				try
				{
					ProtocolFtp.log.debug(ProtocolFtp.this.getTaskid() + ": sleep 开始");
					Thread.sleep(this.nSeconds * 1000L);
					ProtocolFtp.log.debug(ProtocolFtp.this.getTaskid() + ": sleep 结束");
				}
				catch (InterruptedException e)
				{
					ProtocolFtp.log.debug(ProtocolFtp.this.getTaskid() + 
					": Monitor thread interrupted by ftp thread");
					return;
				}

				try
				{
					ProtocolFtp.log.debug("Task " + this.nTaskID + " ftp timeout for " + 
							this.nSeconds + " seconds, interrupt ftp thread");

					this.ftpThread.interrupt();
				}
				catch (Exception e)
				{
					ProtocolFtp.log.error("Interrupt ftp error", e);
				}
			}
		}	
  	}
}