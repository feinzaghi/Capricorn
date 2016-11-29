package com.turk.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.Config.SystemConfig;
import com.turk.DataImport.BCPImport;
import com.turk.DataImport.ColumnObject;
import com.turk.DataImport.IOutput;
import com.turk.DataImport.SqlLoadImport;
import com.turk.Service.LoaderService;
import com.turk.Service.MapService;
import com.turk.Service.TransactionCenter;
import com.turk.Service.root;

public class Import {
	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	private IOutput output = null;
	private static String _LoaderUrl = SystemConfig.getInstance().UteleLoaderUrl();
	private static String _CheckUrl = SystemConfig.getInstance().UteleCheckUrl();
	private static String _LogoutUrl = SystemConfig.getInstance().UteleLogoutUrl();
	//private static String _RemoteRoot=SystemConfig.getInstance().getRemoteRoot();
	
	private static String _checkuser = SystemConfig.getInstance().HttpCheckUser();
	private static String _checkpassword = SystemConfig.getInstance().HttpCheckPassword();
	
	protected String strSplit=";";
	public Import(String split)
	{
		this.strSplit=split;
	}
	
	/**
	 * �����̳߳���⣬�Ƿ�����ִ�����
	 * ע��,���������������ļ�ʱ������������Ϊtrue
	 * @param execimmediate
	 */
	public Import(boolean execimmediate)
	{
		this.executeimmediate=execimmediate;
	}
	
	private String _pathName = "";
	private String _fileName = "";
	private String _cltFileName = "";
	private String _tableName = "";
	private String _timeString = "";
	private String _charSet = "";
	private boolean executeimmediate = false;
	//private FileWriter fileWriter;
	//private BufferedWriter fileoutWriter;
	private FileOutputStream fileoutStream;
	private ColumnObject[] _columns = null;
	

	public void SetFilePath(String pathName) {
		// TODO Auto-generated method stub
		_pathName = pathName;
		
	}

	public void SetFlieName(String fileName,String timeString) {
		// TODO Auto-generated method stub
		_fileName = fileName;
		_timeString = timeString;
		File file = new File(_fileName);
		if(!file.exists())
		{
			try {
				file.createNewFile();
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	public void WriteFile(int KeyID,String data,String tableName) {
		// TODO Auto-generated method stub
		try {
			Open();
			//����д�ļ�
			String[] lineData = data.split("\n");
			for(String line : lineData)
			{
				
				line = line + "\n";
				this.fileoutStream.write(line.getBytes("utf-8"));
				
				//this.fileWriter.write(line + "\n");
			}
			
			Commit(KeyID,tableName);
			Dispose();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void WriteFileLine(String lineString) {
		// TODO Auto-generated method stub
		try {
			lineString = lineString.replace(";", strSplit) + "\n";
			this.fileoutStream.write(lineString.getBytes());
			
			//this.fileWriter.append(lineString.replace(";", strSplit) + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void WriteFileLine(String lineString,String charSet) {
		// TODO Auto-generated method stub
		try {
			lineString = lineString.replace(";", strSplit) + "\n";
			this.fileoutStream.write(lineString.getBytes(charSet));
			
			//this.fileWriter.append(lineString.replace(";", strSplit) + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * �ύ���
	 * @param tableName ������
	 * @throws Exception
	 */
	public void Commit(int KeyID,String tableName) throws Exception
	{
	    try
	    {
	    	_tableName = tableName;
	    }
	    catch (Exception localException)
	    {
	    	log.error("�ļ����رմ���",localException);
	    }
	    finally
	    {
	    	if(this.fileoutStream!=null)
			{
				this.fileoutStream.flush();
				this.fileoutStream.close();
			}
	    	this.fileoutStream = null;
	    }
	
	    //���������ļ��ύ
	    if(Util.isOracle())
	    {
	    	output = new SqlLoadImport();
	    	output.setExecuteImmediate(executeimmediate);
	    }
	    if(Util.isSqlServer())
	    {
	    	output = new BCPImport();
	    }
	    output.ExcuteImport(KeyID,_cltFileName,
				_columns,_tableName,_fileName,_timeString,strSplit);
	}
	
	/**
	 * �ύ���
	 * @param tableName ������
	 * @throws Exception
	 */
	public void Commit(int KeyID,String tableName,
			String DBServer,String userid,String password) throws Exception
	{
	    try
	    {
	    	_tableName = tableName;
	    }
	    catch (Exception localException)
	    {
	    	log.error("�ļ����رմ���",localException);
	    }
	    finally
	    {
	    	if(this.fileoutStream!=null)
			{
				this.fileoutStream.flush();
				this.fileoutStream.close();
			}
	    	this.fileoutStream = null;
	    }
	
	    //���������ļ��ύ
	    if(Util.isOracle())
	    {
	    	output = new SqlLoadImport();
	    	output.setExecuteImmediate(executeimmediate);
	    }
	    if(Util.isSqlServer())
	    {
	    	output = new BCPImport();
	    }
	    output.ExcuteImport(KeyID,DBServer,userid,password,_cltFileName,
				_columns,_tableName,_fileName,_timeString,strSplit);
	}
	
	public void Commit(FTPInfo info,String localFile,String remotePath,String gpRemotePath,String idx) throws Exception
	{
		String fileName=localFile.substring((localFile.lastIndexOf("\\")+1),localFile.length());
		try
	    {
	    	
	    	//this.fileWriter.flush();
	    	//this.fileWriter.close();
			this.fileoutStream.flush();
	    	this.fileoutStream.close();
	    	this.fileoutStream = null;
	    	//---��ʽת��
	    	/*
	    	if(_charSet.equals("UTF-8"))
	    	{
	    	    String filePath = localFile.substring(0,(localFile.lastIndexOf("\\")+1));
	    	    String targetFile = filePath + File.separator + "C_" + fileName;
	    	    ANSITOUTF8(_fileName,targetFile);
	    	    File sourceFile = new File(_fileName);
				if (sourceFile.exists())
				{
					if (sourceFile.delete())
					{
						
					}
				}
				File tagFile = new File(targetFile);
				if(tagFile.exists())
				{
					tagFile.renameTo(new File(_fileName));
				}
	    	}*/
	    	
	    	
	    	
	    	if(SystemConfig.getInstance().isFtp()){
		    	log.info("�ļ�" + fileName + "��ʼ�ϴ�FTP...");
		 		FTPToolCommon ftpobj = new FTPToolCommon(info);
		 		ftpobj.login(2000, 3);
		 		int code = ftpobj.uploadFile(localFile, remotePath);
				switch(code)
				{
				   	case 100://�ɹ�
				   		log.info("�ļ�" + fileName + "�ϴ��ɹ�");
				    	break;
				    case 400:
				    		//�쳣
				    	log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
				    	break;
				    case 401:
				    		//��������ʧ��
				    	log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
				    	break;
				 }
				    	
				ftpobj.disconnect();
				
				
				
				NoticeHttp(info,remotePath,fileName);
				 
				log.info("�ļ�"+fileName+"�ϴ��ɹ�");
				log.info("�ļ�"+fileName+"�ɹ��ϴ���FTP:"+info.getIP()+(remotePath.isEmpty()?"��Ŀ¼":remotePath+"�ļ���")+"��");
	    	}
	    	String ackLocalFile="";
	    	String idxLocalFile="";
	    	if(SystemConfig.getInstance().isGp()){
	    		
	    		FTPInfo gpInfo=new FTPInfo();
	    		gpInfo.setIP(SystemConfig.getInstance().getGpIp());
	    		gpInfo.setPort(Integer.parseInt(SystemConfig.getInstance().getGpPort()));
	    		gpInfo.setUser(SystemConfig.getInstance().getGpUser());
	    		gpInfo.setPwd(SystemConfig.getInstance().getGpPwd());
	    		gpInfo.setEncode(SystemConfig.getInstance().getGpEncoding());
		    	log.info("�ļ�" + fileName + "��ʼ�ϴ�FTP...");
		 		FTPToolCommon ftpobj = new FTPToolCommon(gpInfo);
		 		ftpobj.login(2000, 3);
		 		int code = ftpobj.uploadFile(localFile, gpRemotePath);
				switch(code)
				{
				   	case 100://�ɹ�
				   		log.info("�ļ�" + fileName + "�ϴ��ɹ�");
				    	break;
				    case 400:
				    		//�쳣
				    	log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
				    	break;
				    case 401:
				    		//��������ʧ��
				    	log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
				    	break;
				 }
				
			    ackLocalFile=localFile.substring(0,localFile.lastIndexOf('.'))+".ack";
				File file=new File(ackLocalFile); 
				if(!file.exists())
				{
					try {
						file.createNewFile();
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				FTPToolCommon ftpobj1 = new FTPToolCommon(gpInfo);
		 		ftpobj1.login(2000, 3);
				int ackCode=ftpobj1.uploadFile(ackLocalFile, gpRemotePath);
				switch(ackCode)
				{
				   	case 100://�ɹ�
				   		log.info("�ļ�" + ackLocalFile + "�ϴ��ɹ�");
				    	break;
				    case 400:
				    		//�쳣
				    	log.warn("�ļ�" + ackLocalFile + "�ϴ�ʧ��");
				    	break;
				    case 401:
				    		//��������ʧ��
				    	log.warn("�ļ�" + ackLocalFile + "�ϴ�ʧ��");
				    	break;
				 }
				
				if(!idx.isEmpty()){
					String str=localFile.substring(0, localFile.lastIndexOf("\\")+1);
					idxLocalFile=str+idx+".idx";
					File idxfile=new File(idxLocalFile); 
					if(!idxfile.exists())
					{
						try {
							idxfile.createNewFile();
							
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					
					FTPToolCommon ftpobjidx = new FTPToolCommon(gpInfo);
					ftpobjidx.login(2000, 3);
					int idxCode=ftpobjidx.uploadFile(idxLocalFile, gpRemotePath);
					switch(idxCode)
					{
					   	case 100://�ɹ�
					   		log.info("�ļ�" + idxLocalFile + "�ϴ��ɹ�");
					    	break;
					    case 400:
					    		//�쳣
					    	log.warn("�ļ�" + idxLocalFile + "�ϴ�ʧ��");
					    	break;
					    case 401:
					    		//��������ʧ��
					    	log.warn("�ļ�" + idxLocalFile + "�ϴ�ʧ��");
					    	break;
					 }
					ftpobjidx.disconnect();
				}
				
				ftpobj.disconnect();
				ftpobj1.disconnect();
				//NoticeHttp(gpInfo,remotePath,fileName);
				 
				log.info("�ļ�"+fileName+"�ϴ��ɹ�");
				log.info("�ļ�"+fileName+"�ɹ��ϴ���FTP:"+gpInfo.getIP()+(gpRemotePath.isEmpty()?" ��Ŀ¼":gpRemotePath+" �ļ���")+"��");
	    	}
	    	
			if (SystemConfig.getInstance().isDeleteLog())
			{
				File ctlfile = new File(_cltFileName);
				if (ctlfile.exists()) {
					ctlfile.delete();
				}
				
				File ackFile=new File(ackLocalFile);
				if(ackFile.exists()){
					ackFile.delete();
				}
				
				File idxFile=new File(idxLocalFile);
				if(idxFile.exists()){
					idxFile.delete();
				}
				
				String strTxt = _fileName;
				File txtfile = new File(strTxt);
				if (txtfile.exists())
				{
					if (txtfile.delete())
					{
						this.log.debug("·���������ļ�" + ": " + strTxt + 
						"ɾ���ɹ�....");
					}
					else
					{
						this.log.warn("·���������ļ�" + ": " + strTxt + 
						"ɾ��ʧ��");
					}
				}
				else
				{
					this.log.warn("·���������ļ�" + ": " + strTxt + 
					"δ�ҵ����޷�ɾ��");
				}
			}
	    }
	    catch (Exception localException)
	    {
	    	log.error(localException);
	    	//System.out.println(localException.toString());
	    }
	}
	
	
	public void Commit(String gpRemotePath) throws Exception
	{
		String fileName=_fileName.substring((_fileName.lastIndexOf("\\")+1),_fileName.length());
		try
	    {
			this.fileoutStream.flush();
	    	this.fileoutStream.close();
	    	this.fileoutStream = null;
	    	
	    	String ackLocalFile="";
	    	String idxLocalFile="";
	    	if(SystemConfig.getInstance().isGp()){
	    		
	    		FTPInfo gpInfo=new FTPInfo();
	    		gpInfo.setIP(SystemConfig.getInstance().getGpIp());
	    		gpInfo.setPort(Integer.parseInt(SystemConfig.getInstance().getGpPort()));
	    		gpInfo.setUser(SystemConfig.getInstance().getGpUser());
	    		gpInfo.setPwd(SystemConfig.getInstance().getGpPwd());
	    		gpInfo.setEncode(SystemConfig.getInstance().getGpEncoding());
		    	log.info("�ļ�" + fileName + "��ʼ�ϴ�FTP...");
		 		FTPToolCommon ftpobj = new FTPToolCommon(gpInfo);
		 		ftpobj.login(2000, 3);
		 		int code = ftpobj.uploadFile(_fileName, gpRemotePath);
				switch(code)
				{
				   	case 100://�ɹ�
				   		log.info("�ļ�" + fileName + "�ϴ��ɹ�");
				    	break;
				    case 400:
				    		//�쳣
				    	log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
				    	break;
				    case 401:
				    		//��������ʧ��
				    	log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
				    	break;
				 }
				
			    ackLocalFile=_fileName.substring(0,_fileName.lastIndexOf('.'))+".ack";
				File file=new File(ackLocalFile); 
				if(!file.exists())
				{
					try {
						file.createNewFile();
						
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				FTPToolCommon ftpobj1 = new FTPToolCommon(gpInfo);
		 		ftpobj1.login(2000, 3);
				int ackCode=ftpobj1.uploadFile(ackLocalFile, gpRemotePath);
				switch(ackCode)
				{
				   	case 100://�ɹ�
				   		log.info("�ļ�" + ackLocalFile + "�ϴ��ɹ�");
				    	break;
				    case 400:
				    		//�쳣
				    	log.warn("�ļ�" + ackLocalFile + "�ϴ�ʧ��");
				    	break;
				    case 401:
				    		//��������ʧ��
				    	log.warn("�ļ�" + ackLocalFile + "�ϴ�ʧ��");
				    	break;
				 }
				
				ftpobj.disconnect();
				ftpobj1.disconnect();
				//NoticeHttp(gpInfo,remotePath,fileName);
				 
				log.info("�ļ�"+fileName+"�ϴ��ɹ�");
				log.info("�ļ�"+fileName+"�ɹ��ϴ���FTP:"+gpInfo.getIP()+(gpRemotePath.isEmpty()?" ��Ŀ¼":gpRemotePath+" �ļ���")+"��");
	    	}
	    	
			if (SystemConfig.getInstance().isDeleteLog())
			{
				File ctlfile = new File(_cltFileName);
				if (ctlfile.exists()) {
					ctlfile.delete();
				}
				
				File ackFile=new File(ackLocalFile);
				if(ackFile.exists()){
					ackFile.delete();
				}
				
				File idxFile=new File(idxLocalFile);
				if(idxFile.exists()){
					idxFile.delete();
				}
				
				String strTxt = _fileName;
				File txtfile = new File(strTxt);
				if (txtfile.exists())
				{
					if (txtfile.delete())
					{
						this.log.debug("·���������ļ�" + ": " + strTxt + 
						"ɾ���ɹ�....");
					}
					else
					{
						this.log.warn("·���������ļ�" + ": " + strTxt + 
						"ɾ��ʧ��");
					}
				}
				else
				{
					this.log.warn("·���������ļ�" + ": " + strTxt + 
					"δ�ҵ����޷�ɾ��");
				}
			}
	    }
	    catch (Exception localException)
	    {
	    	log.error(localException);
	    	//System.out.println(localException.toString());
	    }
	}
	
	private boolean NoticeHttp(FTPInfo info,String remotePath,String fileName)
	{
		boolean blResult = false;
		try
		{
			//HttpЭ�����
			com.turk.Service.TransactionCenter trans=new TransactionCenter(info.getEncode(), info.getEncode());
			//Check
			String strCheck = "j_username=" + _checkuser + "&" + "j_password=" + _checkpassword;
			Map<String,String> check = new HashMap();
			check.put("j_username", _checkuser);
			check.put("j_username", _checkpassword);
			
			String CheckUrl = _CheckUrl + "?" + strCheck;
			String checkResult=trans.connect(new HashMap(), CheckUrl);
			
			log.info("HTTP Check��Ϣ:Post-" + strCheck + ";Response:" + checkResult);
			
			root rtcheck = (root)Util.strSerialization(checkResult, root.class);
			
			String sessionid = rtcheck.sessionId;
			
						
			//Send Info
			StringBuffer sb=new StringBuffer();
			sb.append("ftpServer="+info.getIP()+"&");
			sb.append("ftpPort="+info.getPort()+"&");
			sb.append("user="+info.getUser()+"&");
			sb.append("password="+info.getPwd()+"&");
			sb.append("passiveMode="+SystemConfig.getInstance().getPassiveMode()+"&");
			sb.append("encoding="+info.getEncode()+"&");
			sb.append("remoteRoot=./"+remotePath+"/&");
			sb.append("filelist=./"+fileName);
			
			Map<String,String> load = new HashMap();
			load.put("ftpServer", info.getIP());
			load.put("ftpPort", String.valueOf(info.getPort()));
			load.put("user", info.getUser());
			load.put("password", info.getPwd());
			load.put("passiveMode", SystemConfig.getInstance().getPassiveMode());
			load.put("encoding", info.getEncode());
			load.put("remoteRoot", "./" + remotePath + "/");
			load.put("filelist", "./" + fileName);
			
			String LoaderUrl = _LoaderUrl + ";jsessionid=" + sessionid;
			String returnResult=trans.connect(sb.toString(), LoaderUrl);
			log.info("HTTP Load��Ϣ:Post-" + sb.toString() + ";Response:" + returnResult);
			//��Ҫ��鷵���Ƿ�ʧ�ܣ��ȴ����ٴ��ϴ�,Turk
			root rtload = (root)Util.strSerialization(returnResult, root.class);
			if(rtload.status.toUpperCase().equals("SUCCESS"))
			{
				blResult = true;
			}
			
			//Logout
			String strLogout = "";
			Map<String,String> logout = new HashMap();
			String logoutResult=trans.connect(logout, _LogoutUrl);
			
			log.info("HTTP ע����Ϣ:Post-" + strLogout + ";Response:" + logoutResult);
			
			
		}
		catch(Exception ex)
		{
			log.error("HTTP֪ͨ�ӿ��쳣",ex);
		}
		return blResult; 
	}
	
	/**
	 * �ļ�д���ر��ļ�
	 * @param tableName ������
	 * @throws Exception
	 */
	public void Close() throws Exception
	{
	    try
	    {
	    	this.fileoutStream.flush();
	    	this.fileoutStream.close();
	    	//this.fileWriter.flush();
	    	//this.fileWriter.close();
	    	
	    }
	    catch (Exception localException)
	    {
	    	log.error("�ļ��رմ���",localException);
	    }
	}
	
	public void Dispose()
	{
		try
		{
			if(this.fileoutStream!=null)
			{
				this.fileoutStream.flush();
				this.fileoutStream.close();
			}
			
			this.fileoutStream = null;
			this._fileName = null;
		}
		catch (Exception localException)
	    {
			log.error("�ļ��رմ���",localException);
	    }
	}

	/**
	 * ���ļ�
	 * @throws Exception
	 */
	public void Open() throws Exception {
		// TODO Auto-generated method stub
		//this.fileWriter = new FileWriter(this._fileName);
		this.fileoutStream = new FileOutputStream(new File(this._fileName),false);
		//this.fileoutWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this._fileName),"UTF-8"));
	}
	

	
	public void SetCltFileName(String fileName)
	{
		// TODO Auto-generated method stub
		_cltFileName = fileName;
		File file = new File(fileName);
		if(!file.exists())
		{
			try {
				file.createNewFile();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * ����sqlldr��
	 * @param columns
	 */
	public void SetColumns(ColumnObject[] columns)
	{
		_columns = columns;
	}
	
	public void ANSITOUTF8(String sourceFile,String targetFile)
	{
		FileInputStream fis;
		try {
			fis = new FileInputStream(sourceFile);
		
			InputStreamReader isr;
	
			isr = new InputStreamReader(fis,"GBK");
		
	        BufferedReader br = new BufferedReader(isr);
	        FileOutputStream out = new FileOutputStream(new File(targetFile),true);
	        //StringBuffer sb = new StringBuffer();
	        String str = null;
	      
			while((str = br.readLine()) != null)
			{
				str = str + "\n";
				out.write(str.getBytes("utf-8"));
			}
			
	        
	        out.close();
	        br.close();
	        isr.close();
	        fis.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}      catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
