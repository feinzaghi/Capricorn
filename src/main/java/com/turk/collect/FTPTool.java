package com.turk.collect;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import com.turk.datalog.DataFtpLog;
import com.turk.datalog.DataFtpLogInfo;
import com.turk.datalog.DataLogMgr;
import com.turk.task.CollectObjInfo;
import com.turk.task.DevInfo;
import com.turk.task.RegatherObjInfo;
import com.turk.util.LogMgr;
import com.turk.util.Util;

/**
 * FTP����
 * @author Administrator
 *
 */
public class FTPTool
{
	protected String ip;
	protected int port;
	protected String user;
	protected String pwd;
	protected String encode;
	protected CollectObjInfo taskinInfo;
	protected String keyId;
	protected FTPClient ftp;
	protected FTPClientConfig ftpCfg;
	
	protected static Logger logger = LogMgr.getInstance().getSystemLogger();
	
	protected static Logger errorlog = LogMgr.getInstance().getErrorLogger();

	public FTPTool(CollectObjInfo taskInfo)
	{
		DevInfo dev = taskInfo.getDevInfo();
		this.ip = dev.getIP();
		this.port = taskInfo.getDevPort();
		this.user = dev.getHostUser();
    	this.pwd = dev.getHostPwd();
    	this.encode = dev.getEncode();
    	this.taskinInfo = taskInfo;
    	int id = this.taskinInfo.getTaskID();
    	if ((this.taskinInfo instanceof RegatherObjInfo))
    		id = this.taskinInfo.getKeyID() - 10000000;
    	this.keyId = (this.taskinInfo.getTaskID() + "-" + id + " ");
	}
	
	public FTPTool(String IP,int port,String user,String pwd)
	{
		this.ip = IP;
		this.port = port;
		this.user = user;
    	this.pwd = pwd;
	}
	
	public void setKeyID(String keyid)
	{
		this.keyId = keyid + "-" + keyid + " ";
	}

	/**
	 * ��½
	 * @param sleepTime
	 * @param tryTimes
	 * @return
	 */
	public boolean login(int sleepTime, int tryTimes)
	{
		boolean b = false;
		if (login()) 
			return true;
		if (tryTimes > 0)
		{
			for (int i = 0; i < tryTimes; i++)
			{
				if (sleepTime <= 0)
					continue;
				logger.warn(this.keyId + "�������µ�¼������:" + (i + 1));
				try
				{
					Thread.sleep(sleepTime);
				}
				catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				b = login();
				if (!b)
					continue;
				logger.debug(this.keyId + "���µ�¼�ɹ�");
				break;
			}
		}

		if (!b)
		{
			logger.warn(this.keyId + "���µ�¼ʧ��");
		}
		return b;
	}

	/**
	 * �����ļ�
	 * @param ftpPath
	 * @param localPath
	 * @return
	 */
	public DownStructer downFile(String ftpPath, String localPath)
	{
		String aFtpPath = ftpPath;

		if (Util.isNotNull(ftpPath))
		{
			if ((ftpPath.contains("!")) && (ftpPath.contains("{")) && 
					(ftpPath.contains("}")))
			{
				int begin = ftpPath.indexOf("!");
				int end = ftpPath.lastIndexOf("!");
				if ((begin > -1) && (end > -1) && (begin < end))
				{
					String content = ftpPath.substring(begin, end + 1);
					int cBegin = content.indexOf("!");
					int cEnd = content.indexOf("{");
					if ((cBegin > -1) && (cEnd > -1) && (cBegin < cEnd))
					{
						String dir = content.substring(cBegin + 1, cEnd);
						aFtpPath = aFtpPath.replace(content, dir);
					}
				}
			}
		}
		

		FTPFile[] ftpFiles = (FTPFile[])null;
		DownStructer downStruct = new DownStructer();
		
		boolean bOK = this.login(120*1000, 5);
    	if (!bOK)
    	{
    		String logStr = this.taskinInfo.getTaskID() + ":downFile- FTP��γ��Ե�½ʧ��:" + ftp;
    		logger.error(logStr);
	        return downStruct;
    	}
    	
    	logger.debug(this.taskinInfo.getTaskID() + ":downFile- FTP��½�ɹ�.");
    	
		try
		{
			boolean isEx = false;
			try
			{
				ftpFiles = this.ftp.listFiles(encodeFTPPath(aFtpPath));
			}
			catch (Exception e)
			{
				logger.error(this.keyId + "listFilesʧ��:" + aFtpPath, e);
				isEx = true;
			}
			int sleepTime;
			if (!isFilesNotNull(ftpFiles))
			{
				for (int i = 0; i < 3; i++)
				{
					sleepTime = 5000 * (i + 1);
					if (isEx)
					{
						logger.warn(this.keyId + "listFiles�쳣���Ͽ�����");
						login();
					}
					logger.warn(this.keyId + "���³���listFiles: " + aFtpPath + ",����:" + (
							i + 1));
					Thread.sleep(sleepTime);
					try
					{
						ftpFiles = this.ftp.listFiles(encodeFTPPath(aFtpPath));
					}
					catch (Exception e)
					{
						logger.error("listFilesʧ�ܣ�" + aFtpPath, e);
					}
					if (!isFilesNotNull(ftpFiles))
						continue;
					logger.warn(this.keyId + "����listFiles�ɹ���" + aFtpPath);
					break;
				}

				if (!isFilesNotNull(ftpFiles))
				{
					logger.warn(this.keyId + "����3��listFilesʧ�ܣ����ٳ��ԣ�" + aFtpPath);
					return downStruct;
				}
			}
			logger.info(this.keyId + "listFiles�ɹ�,�ļ�����:" + ftpFiles.length + " (" + 
					encodeFTPPath(aFtpPath) + ")");
			for (FTPFile f : ftpFiles)
			{
				if (!f.isFile())
					continue;
				String name = decodeFTPPath(f.getName());
				name = name.substring(name.lastIndexOf("/") + 1, name.length());
				String singlePath = aFtpPath.substring(0, aFtpPath.lastIndexOf("/") + 1) + name;//�ļ�����·��
				String fpath = localPath + File.separator + name.replace(":", "");
				if ((this.taskinInfo.getParserID() == 18) || 
						(this.taskinInfo.getParserID() == 19) || 
						(this.taskinInfo.getParserID() == 4001))
				{	//��Բ�ͬ�������ж�
					singlePath = decodeFTPPath(f.getName());
				}
				
				//�ļ�����·��
				if(this.taskinInfo.getPersistentTask())
				{//����ǳ����Բɼ�������Ҫ�жϵ�ǰ�ļ��Ƿ��ѱ��ɼ� Turk 2012/08/30
					if(DataFtpLog.getInstance(this.taskinInfo.getLastCollectTime()).IsCollected(singlePath))
					{
						continue;
					}
				}
				
				boolean b = downSingleFile(singlePath, localPath, name.replace(":", ""), downStruct);
				if (!b)
				{
					logger.error(this.keyId + "���ص����ļ�ʱʧ��:" + singlePath + ",��ʼ����");
					for (int i = 0; i < 3; i++)
					{
						sleepTime = 2000 * (i + 1);
						logger.warn(this.keyId + "��������:" + singlePath + ",����:" + ( i + 1));
						Thread.sleep(sleepTime);
						login();
						if (!downSingleFile(singlePath, localPath, name.replace(":", ""), downStruct))
							continue;
						b = true;
						logger.warn(this.keyId + "�������سɹ�:" + singlePath);
						break;
					}

					if (!b)
					{
						logger.warn(this.keyId + "����3��ʧ��:" + singlePath);
						downStruct.getFail().add(singlePath);
						continue;
					}

					if (!downStruct.getLocalFail().contains(fpath)) {
						downStruct.getSuc().add(fpath);
					}

				}
				else if (!downStruct.getLocalFail().contains(fpath)) {
					downStruct.getSuc().add(fpath);
				}
				
				if(b)
				{
					//���سɹ���¼�ļ���Ϣ�������Բɼ���Ҫ��¼��Ϣ
					DataFtpLogInfo info = new DataFtpLogInfo();
					info.setTaskID(this.taskinInfo.getTaskID());
					info.setStampTime(this.taskinInfo.getLastCollectTime());
					info.setFileName(singlePath);
					File localFile = new File(localPath,name.replace(":", ""));
					long length = -1;
					if(localFile.exists())
					{
						FileInputStream fis = null; 
						fis = new FileInputStream(localFile); 
						length = fis.available(); 
						//length = (double)fileBytes/(double)1024;
						//fileBytes = Util.round(fileBytes, 3, BigDecimal.ROUND_HALF_DOWN);
						fis.close();
					}
					info.setDeviceID(this.taskinInfo.getDevInfo().getDevID());
					info.setFileSize(length);
					DataLogMgr.getInstance().addFtpLog(info);
				}
			}

		}
		catch (Exception e)
		{
			errorlog.error(this.keyId + "�����ļ�ʱ�쳣", e);
		}
		finally
		{
			try {
				ftp.disconnect();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return downStruct;
	}
	
	/**
	 * �ϴ��ļ�
	 * @param localFile 
	 * @param ftpPath �ϴ�·��
	 * @return ����Code 100:�ɹ���400�쳣ʧ�ܣ�401����3�κ�ʧ��
	 */
	public int uploadFile(String localFile,String ftpPath)
	{
		int returnCode = 400;
		FileInputStream in = null;
		try {
			
			int ftpReplyCode = 0;
			File file = new File(localFile);
			//���뵱ǰĿ¼
			//�ж�Ŀ¼��
			String[] folders = ftpPath.split("/",-1);
			for(String folder:folders)
			{
				if(folder.equals(""))
					continue;
				
				ftpReplyCode = this.ftp.sendCommand(encodeFTPPath("CMD " + folder));
				if (!FTPReply.isPositiveCompletion(ftpReplyCode)) {  
					//�������ڸ�·���������ļ���
					ftpReplyCode = this.ftp.sendCommand(encodeFTPPath("MKD " + folder));
					if (FTPReply.isPositiveCompletion(ftpReplyCode))
					{
						ftpReplyCode = this.ftp.sendCommand(encodeFTPPath("CMD " + folder));
					}
			    }
				this.ftp.changeWorkingDirectory(folder);
			}
			
			//ftpReplyCode = this.ftp.sendCommand(encodeFTPPath("PUT " + localFile));
			in = new FileInputStream(file);
			boolean blreturn = this.ftp.storeFile(file.getName(), in);
			
			String newName = file.getName().replace(".tmp", ".txt");
			blreturn = this.ftp.rename(file.getName(), newName);
			int retryNum = 0;//���Դ���
			while(!blreturn)
			{
				if(retryNum == 3)
				{
					returnCode = 401;
					break;
				}
				retryNum++;
				logger.warn("�ļ�:" + localFile + "�ϴ�ʧ�ܣ�5����" + retryNum + "������");
				
				Thread.sleep(5000L);
				blreturn = this.ftp.storeFile(file.getName(), in);
				newName = file.getName().replace(".tmp", ".txt");
				blreturn = this.ftp.rename(file.getName(), newName);
				
			}
			
		
			if(blreturn)
			{
				logger.debug("File:" + file.getName() + " Upload Success.File size:" + file.getTotalSpace());
				returnCode = 100;
				
			}
		}
		
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			errorlog.error("�ϴ��ļ�ʱ����δ�ҵ��ļ�", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			errorlog.error("�ϴ��ļ�ʱ����", e);
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			errorlog.error("�ϴ��ļ�ʱ����", e);
		}finally
		{
			try {
				in.close();
				//ftp.disconnect();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				errorlog.error("�ϴ��ļ�ʱ����", e);
			}
			in = null;
		}
		return returnCode;
	}

	public FTPClient getFtpClient()
	{
		return this.ftp;
	}

	/**
	 * �Ͽ�����
	 */
	public void disconnect()
	{
		if (this.ftp != null)
		{
			try
			{
				this.ftp.logout();
			}
			catch (Exception localException)
			{
			}
			try
			{
				this.ftp.disconnect();
			}
			catch (Exception localException1)
			{
			}
			this.ftp = null;
		}
	}

	/**
	 * ��½
	 * @return
	 */
	private boolean login()
	{	
		disconnect();
		this.ftp = new MyFTPClient();
		int timeout = 0;
		if(this.taskinInfo!=null)
		{
			timeout = this.taskinInfo.getCollectTime() < 1 ? 5 : this.taskinInfo.getCollectTimeOut();
		}
		else
		{
			timeout = 5;
		}
		this.ftp.setDataTimeout(timeout * 3600 * 1000);
		this.ftp.setDefaultTimeout(timeout * 3600 * 1000);
		boolean b = false;
		try
		{
			logger.debug(this.keyId + "�������ӵ� - " + this.ip + ":" + this.port);
			this.ftp.connect(this.ip, this.port);
			int reply = ftp.getReplyCode();  
	        if (FTPReply.isPositiveCompletion(reply)) {  
	        	logger.debug(this.keyId + "ftp connected");
				logger.debug(this.keyId + "���ڽ��а�ȫ��֤ - " + this.user + " " + this.pwd);
				b = this.ftp.login(this.user, this.pwd);
				logger.debug(this.keyId + "ftp logged in");
				logger.debug(this.keyId + "ftp connected reply code:"+reply + " msg:" + ftp.getReplyString());
	        }
	        else
	        {
	        	
	        	logger.debug(this.keyId + "ftp connected failure code:"+reply + " msg:" + ftp.getReplyString());
	        }
			
		}
		catch (Exception e)
		{
			logger.error(this.keyId + "��¼FTP������ʱ�쳣", e);
			b = false;
		}
		if (b)
		{
			this.ftp.enterLocalPassiveMode();
      		logger.debug(this.keyId + "ftp entering passive mode");
      		if (this.ftpCfg == null)
      		{
      			this.ftpCfg = setFTPClientConfig();
      		}
      		else
      		{
      			this.ftp.configure(this.ftpCfg);
      		}
      		try
      		{
      			this.ftp.setFileType(2);
      		}
      		catch (IOException e)
      		{
      			logger.error("FTP��¼�쳣",e);
      			b = false;
      		}
		}
		return b;
	}

	
	/*private boolean isPositiveCompletion()
		throws IOException
	{
		if (this.ftp == null) 
			return false;
		return this.ftp.completePendingCommand();
	}*/

	/**
	 * �ж��ļ��Ƿ�Ϊ��
	 * @param fs
	 * @return
	 */
	private boolean isFilesNotNull(FTPFile[] fs)
	{
		if (fs == null) 
			return false;
		if (fs.length == 0) 
			return false;
		for (FTPFile f : fs)
		{
			if (f == null) return false;
		}	
		return true;
	}

	/**
	 * ����FTP�����ʽ
	 * @param ftpPath
	 * @return
	 */
	private String encodeFTPPath(String ftpPath)
	{
		try
		{
			String str = Util.isNotNull(this.encode) ? new String(ftpPath.getBytes(this.encode), "iso_8859_1") : ftpPath;
			return str;
		}
		catch (UnsupportedEncodingException e)
		{
			errorlog.error(this.keyId + "���õı��벻��ȷ:" + this.encode, e);
		}
		return ftpPath;
	}

	private String decodeFTPPath(String ftpPath)
	{
		try
		{
			String str = Util.isNotNull(this.encode) ? new String(ftpPath.getBytes("iso_8859_1"), this.encode) : ftpPath;
			return str;
		}
		catch (UnsupportedEncodingException e)
    	{
			errorlog.error(this.keyId + "���õı��벻��ȷ:" + this.encode, e);
    	}
		return ftpPath;
	}

	/**
	 * ���ص����ļ�
	 * @param path
	 * @param localPath
	 * @param fileName
	 * @param downStruct
	 * @return
	 */
	private boolean downSingleFile(String path, String localPath, String fileName, DownStructer downStruct)
	{
		boolean result = false;
		boolean ex = false;
		logger.debug(this.keyId + "��ʼ����:" + path);
		boolean end = true;
    	String singlePath = encodeFTPPath(path);
    	File tdFile = null;
    	InputStream in = null;
    	OutputStream out = null;
    	long length = getFileSize(path);
    	if (length < 0L)
    	{
    		logger.warn("length=" + length); 
    	} 
    	long tdLength = 0L;
    	File f;
    	boolean bRename;
    	try { 
    		File dir = new File(localPath);
    		if (!dir.exists())
    		{
    			if (!dir.mkdirs())
    				throw new Exception(this.keyId + "�����ļ���ʱ�쳣:" + 
    						dir.getAbsolutePath());
    		}
    		tdFile = new File(dir, fileName + 
    				".td_" + Util.getDateString_yyyyMMddHH(this.taskinInfo.getLastCollectTime()));
    		if (!tdFile.exists())
    		{
    			if (!tdFile.createNewFile())
    				throw new Exception(this.keyId + 
    						"������ʱ�ļ�ʧ��:" + tdFile.getAbsolutePath());
    		}
    		tdLength = tdFile.length();
    		if (tdLength >= length)
    		{
    			end = true;
    		}
    		in = this.ftp.retrieveFileStream(singlePath);
    		if (tdLength > -1L)
    		{
    			in.skip(tdLength);
    		}
    		out = new FileOutputStream(tdFile, true);
    		byte[] bytes = new byte[1024];
    		int c;
    		while ((c = in.read(bytes)) != -1)
    		{
    			out.write(bytes, 0, c);
    		}
    		if (tdFile.length() < length)
    		{
    			end = false;
    			logger.warn(this.keyId + tdFile.getAbsoluteFile() + ":�ļ����ز����������۳���:" + 
    					length + "��ʵ�����س���:" + tdFile.length());
    		}
    	}
    	catch (Exception e)
    	{
    		ex = true;
    		errorlog.error(this.keyId + "���ص����ļ�ʱ�쳣:" + path, e);
    		result = false;
    	}
    	finally
    	{
    		if (in != null)
    		{
    			try
    			{
    				in.close();
    			}
    			catch (IOException localIOException3)
    			{
    			}
    		}
    		try
    		{
    			this.ftp.completePendingCommand();
    		}
    		catch (IOException localIOException4)
    		{
    		}
    		if (out != null)
    		{
    			try
    			{
    				out.flush();
    				out.close();
    			}
    			catch (IOException localIOException5)
    			{
    			}
    		}
    		if ((!ex) && ((end) || (tdLength < 0L)))
    		{
    			if (in != null)
    			{
    				f = new File(localPath, fileName);
    				if (f.exists())
    				{
    					f.delete();
    				}
    				bRename = tdFile.renameTo(f);
    				if (!bRename)
    				{
    					errorlog.error(this.keyId + "��" + tdFile.getAbsolutePath() + 
    							"������Ϊ" + f.getAbsolutePath() + "ʱʧ�ܣ�" + 
    							f.getAbsolutePath() + "��ռ��");
    				}
    				else
    				{
    					tdFile.delete();
    					logger.debug(this.keyId + "���سɹ�:" + path + "  ����·��:" + 
    							f.getAbsolutePath() + " �ļ���С:" + f.length());
    					result = true;
            
    					if (f.length() == 0L)
    					{
    						if (!downStruct.getFail().contains(singlePath))
    							downStruct.getFail().add(singlePath);
    						if (downStruct.getLocalFail().contains(f.getAbsolutePath()))
    							downStruct.getLocalFail().add(f.getAbsolutePath());
    						errorlog.error(this.keyId + ": �ļ� " + f.getAbsolutePath() + " ����Ϊ0");
    						return false;
    					}
    				}
    			}
    			else
    			{
    				result = false;
    			}
    		}
    	}

    	return result;
	}

	private long getFileSize(String path)
	{
		try
		{
			FTPFile[] fs = this.ftp.listFiles(encodeFTPPath(path));
			if (!isFilesNotNull(fs)) 
				return -1L;
			for (FTPFile f : fs)
			{
				String name = f.getName();
				name = name.substring(name.lastIndexOf("/") + 1, name.length());
				if ((!name.equals(".")) && (!name.equals(".."))) 
					return f.getSize();
			}
		}
		catch (Exception localException)
		{
		}
		return -1L;
	}
	
	public boolean DeleteFile(String filepath)
	{
		try {
			return this.ftp.deleteFile(filepath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			errorlog.error("ɾ��FTP�ϵ��ļ��쳣",e);
			return false;
		}
	}

 	private FTPClientConfig setFTPClientConfig()
 	{
 		FTPClientConfig cfg = null;
 		try
 		{
 			this.ftp.configure(cfg = new FTPClientConfig("UNIX"));
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("WINDOWS"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:UNIX");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("AS/400"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:WINDOWS");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("TYPE: L8"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:AS/400");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("MVS"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:TYPE: L8");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("NETWARE"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:MVS");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("OS/2"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:NETWARE");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("OS/400"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:OS/2");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("VMS"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:OS/400");
 				return cfg;
 			}
 			
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("UNIX"));
 			}
 			else
 			{
 				logger.debug("FTP Client Config:VMS");
 				return cfg;
 			}
 			logger.debug("FTP Client Config:ALL Failed UNIX");
 		}
 		catch (Exception e)
 		{
 			logger.error("����FTP�ͻ���ʱ�쳣", e);
 			this.ftp.configure(cfg = new FTPClientConfig("UNIX"));
 			//try {
			//	throw e;
			//} catch (Exception e1) {
			//	// TODO Auto-generated catch block
			//	e1.printStackTrace();
			//}
 		}
 		return cfg;
 	}
 	
 	/**
 	 * ����ͨ������г���Ҫ�ɼ����ļ�Ŀ¼
 	 * @param collectPath
 	 * @param ip
 	 * @param port
 	 * @param user
 	 * @param pwd
 	 * @param encode
 	 * @param parserId
 	 * @return
 	 * @throws Exception
 	 */
 	public List<String> listFTPDirs(String collectPath, String ip, int port, String user, String pwd, String encode, int parserId)
		throws Exception
	{
		return listFTPDirs(collectPath, ip, port, user, pwd, encode, true, parserId);
	}
 	
 	/**
  	 * ����ͨ������г���Ҫ�ɼ����ļ�Ŀ¼
  	 * @param collectPath
  	 * @param ip
  	 * @param port
  	 * @param user
  	 * @param pwd
  	 * @param encode
  	 * @param reConnect
  	 * @param parserId
  	 * @return
  	 * @throws Exception
  	 */
	public List<String> listFTPDirs(String collectPath, String ip, int port, String user, String pwd, String encode, boolean reConnect, int parserId)
    	throws Exception
	{
		List<String> result = new ArrayList<String>();
		String tmp = collectPath.substring(0, collectPath.lastIndexOf("/"));
		if ((!tmp.contains("*")) && (!tmp.contains("?")))
		{
			result.add(collectPath);
			return result;
		}
		if ((parserId == 18) || (parserId == 19) || (parserId == 4001))
		{
			result.add(collectPath);
			return result;
		}

		try
		{
			boolean bOK = login(120*1000, 5);
	    	if (!bOK)
	    	{
	    		String logStr = this.taskinInfo.getTaskID() + ":listFTPDirs FTP��γ��Ե�½ʧ��:" + ftp;
	    		errorlog.error(logStr);
		        return result;
	    	}
	    	
	    	logger.debug(this.taskinInfo.getTaskID() + ":listFTPDirs FTP��½�ɹ�.");
	    	
			String[] split = collectPath.split("/");

			List<String> temp = new ArrayList<String>();
			FTPFile[] fs = null;
			for (int i = 0; i < split.length; i++)
			{
				String s = split[i];
				if (!Util.isNotNull(s))
					continue;
				if (!s.startsWith("/"))
				{
					s = "/" + s;
				}
				String str;
				if (temp.size() > 0)
				{
					List<String> temp2 = new ArrayList<String>(temp);
					temp.clear();
					for (String t : temp2)
					{
						str = t + s;
						if ((!s.contains("*")) && (!s.contains("?")))
						{
							result.add(str);
							temp.add(str);
						}
						else 
						{
							str = Util.isNotNull(encode) ? new String(str.getBytes(encode), "iso_8859_1") : str;
							logger.debug(this.taskinInfo.getTaskID() + ":listing - " + str);
						
							try
							{
								fs = this.ftp.listFiles(str);
							}
							catch(Exception e)
							{
								ftp.disconnect();
								logger.warn(this.taskinInfo.getTaskID() + ":list file error,waiting 60s try again",e);
								Thread.sleep(60000);
								if(login())
								{
									try
									{
										logger.debug(this.taskinInfo.getTaskID() + ":re list file time 1");
										fs = ftp.listFiles(str);
									}
									catch(Exception e1)
									{
										logger.error(this.taskinInfo.getTaskID() + ":re list file error!",e);
										break;
									}
									if (fs.length > 0)
									{
										break;
									}
								}
							}
							
							
							int delay;
							if (fs != null && (fs.length == 0) && (reConnect))
							{
								for (int times = 0; times < 3; times++)
								{
									ftp.disconnect();
									delay = (times + 1) * 3000;
									Thread.sleep(delay);
									if(login())
									{
										try
										{
											fs = ftp.listFiles(str);
											if (fs.length > 0)
											{
												break;
											}
										}
										catch(Exception ex)
										{
											continue;
										}
									}
								}
							}
							
							if(fs == null)
							{
								logger.debug(this.taskinInfo.getTaskID() + ":listing - " + str + " δ�����ļ�");
								return result;
							}
							
							if(fs.length == 0)
							{
								logger.debug(this.taskinInfo.getTaskID() + ":δ�ҵ�Ŀ¼:" + str);
							}
							
							for (FTPFile f : fs)
							{
								logger.debug(this.taskinInfo.getTaskID() + ":�ҵ�Ŀ¼ - " + f.getName() + 
										"    parent - " + str + "  raw - " + 
	        						f.getRawListing());
								if ((!f.isDirectory()) || 
										(f.getName().endsWith(".")) || 
										(f.getName().endsWith("..")))
									continue;
								String name = f.getName();
								name = name.substring(name.lastIndexOf("/") + 1, name.length());
								name = Util.isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
								if (!name.startsWith("/"))
								{
									result.add(t + "/" + name);
									temp.add(t + "/" + name);
								}
								else
								{
									result.add(t + name);
									temp.add(t + name);
								}
							}
	           	 		}
	
					}
	
				}
				else if ((!s.contains("*")) && (!s.contains("?")))
				{
					result.add(s);
					temp.add(s);
				}
				else 
				{
					s = Util.isNotNull(encode) ? new String(s.getBytes(encode), "iso_8859_1") : s;
	
					logger.debug(this.taskinInfo.getTaskID() + ":listing(s) - " + s);
					fs = ftp.listFiles(s);
					//log.debug("listFiles-ReplyCode:" + ftp.getReplyCode());
					int delay;
					if ((fs.length == 0) && (reConnect))
					{
						for (int times = 0; times < 3; times++)
						{
							delay = (times + 1) * 1500;
							Thread.sleep(delay);
							ftp.disconnect();
							Thread.sleep(500L);
							ftp.connect(ip, port);
							ftp.login(user, pwd);

							if (this.ftpCfg == null)
				      		{
				      			this.ftpCfg = setFTPClientConfig();
				      		}
				      		else
				      		{
				      			this.ftp.configure(this.ftpCfg);
				      		}
							
							ftp.enterLocalPassiveMode();
							ftp.setFileType(2);
							ftp.setDataTimeout(3600000);
							ftp.setDefaultTimeout(3600000);
							fs = ftp.listFiles(s);
							if (fs.length > 0)
							{
								break;
							}
						}
						
						if(fs.length == 0)
						{
							logger.debug(this.taskinInfo.getTaskID() + ":δ�ҵ�Ŀ¼:" + s);
						}
					}
					
					
					for (FTPFile f : fs)
					{
						if ((!f.isDirectory()) || (f.getName().equals(".")) || 
								(f.getName().equals("..")))
							continue;
						String name = f.getName();
						name = Util.isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
						result.add("/" + name);
						temp.add("/" + name);
					}
	
				}
	
			}

			List<String> tmpResult = new ArrayList<String>(result);

			for (String s : result)
			{
				if (s.split("/").length == split.length - 1)
					continue;
				tmpResult.remove(s);
			}

			result.clear();

			for (String s : tmpResult)
			{
				result.add(s + "/" + split[(split.length - 1)]);
			}
		}
		catch(Exception e)
		{
			errorlog.error(this.taskinInfo.getTaskID() + ":listFTPDirs error.",e);
		}
		finally
		{
			ftp.disconnect();
		}
		return result;
	}
}