package com.turk.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.*;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.log4j.Logger;

import com.turk.Service.MapService;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class FTPToolCommon {
	protected String ip;
	protected int port;
	protected String user;
	protected String pwd;
	protected String encode;
	protected String keyId;
	protected FTPClient ftp;
	protected FTPClientConfig ftpCfg;
	protected static Logger logger = LogMgr.getInstance().getSystemLogger();

	public FTPToolCommon(FTPInfo info)
	{
		this.ip = info.getIP();
		this.port = info.getPort();
		this.user = info.getUser();
    	this.pwd = info.getPwd();
    	this.encode = info.getEncode();
	}
	
	public FTPToolCommon(String IP,int port,String user,String pwd)
	{
		this.ip = IP;
		this.port = port;
		this.user = user;
    	this.pwd = pwd;
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
					sleepTime = 2000 * (i + 1);
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
				String singlePath = aFtpPath.substring(0, aFtpPath.lastIndexOf("/") + 1) + name;
				String fpath = localPath + File.separator + name.replace(":", "");
				//if ((this.taskinInfo.getParserID() == 18) || 
				//		(this.taskinInfo.getParserID() == 19) || 
				//		(this.taskinInfo.getParserID() == 4001))
				//{
				//	singlePath = decodeFTPPath(f.getName());
				//}
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
						return downStruct;
					}

					if (!downStruct.getLocalFail().contains(fpath)) {
						downStruct.getSuc().add(fpath);
					}

				}
				else if (!downStruct.getLocalFail().contains(fpath)) {
					downStruct.getSuc().add(fpath);
				}
			}

		}
		catch (Exception e)
		{
			logger.error(this.keyId + "�����ļ�ʱ�쳣", e);
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
			//�ϴ�֮ǰ�Ȱ��ļ�����Ϊ.tmp
			//String tmpName=file.getName()+".tmp";
			//ftp.setFileType(FTP.BINARY_FILE_TYPE);
			
			boolean blreturn = this.ftp.storeFile(file.getName(), in);
			//�ϴ�����ڸ�Ϊԭ��
			//ftp.rename(tmpName, file.getName());
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
				//ftp.rename(tmpName, file.getName());
				
			}
			in.close();

			if(blreturn)
			{
				returnCode = 100;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			logger.error("�ϴ��ļ�ʱ����δ�ҵ��ļ�", e);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("�ϴ��ļ�ʱ����", e);
		}catch (InterruptedException e) {
			// TODO Auto-generated catch block
			logger.error("�ϴ��ļ�ʱ����", e);
		}finally
		{
			
		}
		return returnCode;
	}
	
	/**
	 * ɾ��FTP�ϵ��ļ�
	 * @param filePath
	 * @return
	 */
	public boolean DeleteFile(String filePath)
	{
		try {
			return this.ftp.deleteFile(filePath);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("ɾ���ļ�����",e);
			return false;
		}
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
		
		timeout = 5;
		
		this.ftp.setDataTimeout(timeout * 60 * 1000);
		this.ftp.setDefaultTimeout(timeout * 60 * 1000);
		boolean b = false;
		try
		{
			logger.debug(this.keyId + "�������ӵ� - " + this.ip + ":" + this.port);
			this.ftp.connect(this.ip, this.port);
			logger.debug(this.keyId + "ftp connected");
			logger.debug(this.keyId + "���ڽ��а�ȫ��֤ - " + this.user + " " + this.pwd);
			b = this.ftp.login(this.user, this.pwd);
			logger.debug(this.keyId + "ftp logged in");
		}
		catch (Exception e)
		{
			logger.error(this.keyId + "��¼FTP������ʱ�쳣", e);
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
      			e.printStackTrace();
      		}
		}
		return b;
	}

	private boolean isPositiveCompletion()
		throws IOException
	{
		if (this.ftp == null) 
			return false;
		return this.ftp.completePendingCommand();
	}

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
			logger.error(this.keyId + "���õı��벻��ȷ:" + this.encode, e);
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
			logger.error(this.keyId + "���õı��벻��ȷ:" + this.encode, e);
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
    		logger.warn("lenght=" + length); } 
    		long tdLength = 0L;
    		File f;
    		boolean bRename;
    		try { File dir = new File(localPath);
    		if (!dir.exists())
    		{
    			if (!dir.mkdirs())
    				throw new Exception(this.keyId + "�����ļ���ʱ�쳣:" + 
    						dir.getAbsolutePath());
    		}
    		tdFile = new File(dir, fileName + 
    				".td_" + Util.getDateString_yyyyMMddHH(new Date()));
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
    		logger.error(this.keyId + "���ص����ļ�ʱ�쳣:" + path, e);
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
    					logger.error(this.keyId + "��" + tdFile.getAbsolutePath() + 
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
    						logger.error(this.keyId + ": �ļ� " + f.getAbsolutePath() + " ����Ϊ0");
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
 				logger.debug(this.keyId + "ftp type:UNIX");
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("AS/400"));
 			}
 			else
 			{
 				logger.debug(this.keyId + "ftp type:NT");
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("TYPE: L8"));
 			}
 			else
 			{
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("MVS"));
 			}
 			else
 			{
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("NETWARE"));
 			}
 			else
 			{
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("OS/2"));
 			}
 			else
 			{
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("OS/400"));
 			}
 			else
 			{
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("VMS"));
 			}
 			else
 			{
 				return cfg;
 			}
 			if (!isFilesNotNull(this.ftp.listFiles("/*")))
 			{
 				this.ftp.configure(cfg = new FTPClientConfig("UNIX"));
 			}
 		}
 		catch (Exception e)
 		{
 			logger.error("����FTP�ͻ���ʱ�쳣", e);
 			this.ftp.configure(cfg = new FTPClientConfig("UNIX"));
 		}
 		return cfg;
 	}

 	public static void main(String[] args) throws Exception
 	{
 		//CollectObjInfo info = new CollectObjInfo(123);
 		//info.setLastCollectTime(new Timestamp(Util.getDate1("2011-01-01 12:00:00").getTime()));
 		FTPInfo info = new FTPInfo();
 		info.setIP("192.168.0.151");
 		info.setPort(21);
 		info.setUser("java");
 		info.setPwd("java123");
 		info.setEncode("UTF-8");
 		
 		FTPToolCommon ftp = new FTPToolCommon(info);
 		ftp.login(2000, 3);// /20/CDR/GZBSC1/export/cdr_hw_1x_20_*201304082355*.txt
 		int code = ftp.uploadFile("D:\\temp\\cdr_hw_1x_20_1_20130408235500.txt", "/cdr/1x");
 		logger.info("file code:"+code);
		    	
		 ftp.disconnect();
 	}
}


