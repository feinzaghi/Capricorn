package com.turk.distributor;

import java.io.File;
import java.io.FileInputStream;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.collect.FTPTool;
import com.turk.task.CollectObjInfo;
import com.turk.Config.SystemConfig;
import com.turk.util.LogMgr;

/**
 * �ļ���ʽ�ַ�
 * @author Administrator
 *
 */
public class DistributeFile {
	
	private CollectObjInfo collectInfo;
	private DistributeTemplet disTmp;
	private Logger log = LogMgr.getInstance().getSystemLogger();
    private Thread mainThread;
    
	public DistributeFile(CollectObjInfo ColInfo)
	{
		this.collectInfo = ColInfo;
		this.disTmp = ((DistributeTemplet)this.collectInfo.getDistributeTemplet());
	}
	
	
	public void BulidFileUpoadFtpThread(final int tableIndex, final String tempFile)
	{
		Map<Integer, TableItem> tableItems = this.disTmp.tableItems;
		final TableItem tableItem = tableItems.get(Integer.valueOf(tableIndex));
		
		this.mainThread = new Thread(new Runnable()
 		{
 			public void run()
 			{
 				BuildFileUploadFtp(tableItem.outputFileName,tableIndex,tempFile);
 			}
 		});
 		
 		this.mainThread.start();
	}
	
	
	/**
	 * �ļ��ַ���FTP
	 * @param tableIndex
	 * @param tempFile
	 */
	public void BuildFileUploadFtp(int tableIndex, String tempFile)
	{
		String logStr = "";
		//�õ���ǰ��ķַ�ģ��
		DistributeTemplet.TableTemplet TableInfo 
			= (DistributeTemplet.TableTemplet)this.disTmp.tableTemplets.get(Integer.valueOf(tableIndex));
		
		Map<Integer, TableItem> tableItems = this.disTmp.tableItems;
		TableItem tableItem = tableItems.get(Integer.valueOf(tableIndex));
		
		//if(!TableInfo.UploadPath.isEmpty())
		//{
			String uploadPath = TableInfo.UploadPath;
			//���������ļ��ϴ���ָ��Ŀ¼
			//
			
			//if(uploadPath.toLowerCase().contains("ftp"))
			{ //FTP ��ַ������FTP�ϴ�
				String ftpIP = collectInfo.getInDBServerConfig().getInDBServer();
				String ftpuser = collectInfo.getInDBServerConfig().getInDBUser();
				String ftppwd = collectInfo.getInDBServerConfig().getInDBPassword();
				FTPTool ftp = new FTPTool(ftpIP,21,ftpuser,ftppwd);
				
				//�ѵ�ǰ�ɼ�����������Ϊ�ɼ�Ŀ¼
				String docName = uploadPath;
				ftp.setKeyID(String.valueOf(collectInfo.getTaskID()));
				
				
				logStr = "�ַ����ݣ���ʼFTP��½.";
				this.log.debug(logStr);
				try
				{
					String fileName = tableItem.outputFileName;
				    File file = new File(fileName);
				    
				    
				    
				    String sName = file.getName();
				    sName = sName.substring(0,sName.indexOf("."));
				    String renameFileName = SystemConfig.getInstance().getCurrentPath() + File.separatorChar 
				        + TableInfo.tableName.toLowerCase() + "_" + sName + ".tmp";
				    String AckFileName = SystemConfig.getInstance().getCurrentPath() + File.separatorChar 
				    	+ TableInfo.tableName.toLowerCase() + "_" + sName + ".ack";
				    
				    boolean blrenamefile = file.renameTo(new File(renameFileName));
				    this.log.debug("rename file:" +  renameFileName + "-" + blrenamefile);
				    
				  //�ж��ļ���С������Ϊ0�����ϴ�
				    @SuppressWarnings("resource")
					long fileSize = new FileInputStream(renameFileName).available();
				    if(fileSize == 0)
				    {
				    	log.debug("File["+ renameFileName +"] Size=0");
				    	File delfile = new File(renameFileName);
				    	if(delfile.delete())
			    		{
			    			log.debug("File:[" + renameFileName + " ][0] Delete done!");
			    		}
				    	return;
				    }
				    
				    
				    boolean bOK = ftp.login(30000, 5);
					if (!bOK)
				 	{
						logStr = "�ַ�����: FTP��γ��Ե�½ʧ��:" + ftp;
						this.log.error(logStr);
						return;
				 	}
				    logStr = "�ַ�����: FTP��½�ɹ�.";
				    this.log.debug(logStr);
				    
				    int code = ftp.uploadFile(renameFileName, docName);
				    switch(code)
				    {
				    	case 100://�ɹ�
				    		log.debug("FTP Upload Code:100-"+renameFileName);
				    		break;
				    	case 400:
				    		//�쳣
				    		log.debug("FTP Upload Code:400-"+renameFileName);
				    		break;
				    	case 401:
				    		//��������ʧ��
				    		log.debug("FTP Upload Code:401-"+renameFileName);
				    		break;
				    }
				    
				    File sucfile = new File(renameFileName);
		    		if(sucfile.delete())
		    		{
		    			log.debug("�ļ�:[" + renameFileName + " ]ɾ���ɹ�");
		    		}
		    		
		    		File ackfile = new File(AckFileName);
				    ackfile.createNewFile();
		    		code = ftp.uploadFile(ackfile.getAbsolutePath(), docName);
		    		if(ackfile.delete())
		    		{
		    			log.debug("�ļ�:[" + AckFileName + " ]ɾ���ɹ�");
		    		}
				    	
				}
  				catch (Exception e)
  			    {
  			    	logStr = "�ַ�����: FTP�ɼ��쳣.";
  			    	this.log.error(logStr, e);
  			    }
  			    finally
  			    {
  			    	ftp.disconnect();
  			    }

			//}
			//else
			{
					//����ڱ���Ŀ¼
			}
				
		}
	}
	
	
	/**
	 * �ļ��ַ���FTP
	 * @param tableIndex
	 * @param tempFile
	 */
	private void BuildFileUploadFtp(String FileName,int tableIndex, String tempFile)
	{
		String logStr = "";
		//�õ���ǰ��ķַ�ģ��
		DistributeTemplet.TableTemplet TableInfo 
			= (DistributeTemplet.TableTemplet)this.disTmp.tableTemplets.get(Integer.valueOf(tableIndex));
		
		//Map<Integer, TableItem> tableItems = this.disTmp.tableItems;
		//TableItem tableItem = tableItems.get(Integer.valueOf(tableIndex));
		
		//if(!TableInfo.UploadPath.isEmpty())
		//{
			String uploadPath = TableInfo.UploadPath;
			//���������ļ��ϴ���ָ��Ŀ¼
			//
			
			//if(uploadPath.toLowerCase().contains("ftp"))
			{ //FTP ��ַ������FTP�ϴ�
				String ftpIP = collectInfo.getInDBServerConfig().getInDBServer();
				String ftpuser = collectInfo.getInDBServerConfig().getInDBUser();
				String ftppwd = collectInfo.getInDBServerConfig().getInDBPassword();
				FTPTool ftp = new FTPTool(ftpIP,21,ftpuser,ftppwd);
				
				//�ѵ�ǰ�ɼ�����������Ϊ�ɼ�Ŀ¼
				String docName = uploadPath;
				ftp.setKeyID(String.valueOf(collectInfo.getTaskID()));
				
				
				logStr = "�ַ����ݣ���ʼFTP��½.";
				this.log.debug(logStr);
				try
				{
					boolean bOK = ftp.login(30000, 5);
					if (!bOK)
				 	{
						logStr = "�ַ�����: FTP��γ��Ե�½ʧ��:" + ftp;
						this.log.error(logStr);
						return;
				 	}
				    logStr = "�ַ�����: FTP��½�ɹ�.";
				    this.log.debug(logStr);
				    
				    String fileName = FileName;
				    File file = new File(fileName);
				    String sName = file.getName();
				    sName = sName.substring(0,sName.indexOf("."));
				    String renameFileName = SystemConfig.getInstance().getCurrentPath() + File.separatorChar 
				        + TableInfo.tableName.toLowerCase() + "_" + sName + ".tmp";
				    String AckFileName = SystemConfig.getInstance().getCurrentPath() + File.separatorChar 
				    	+ TableInfo.tableName.toLowerCase() + "_" + sName + ".ack";
				    
				    boolean blrenamefile = file.renameTo(new File(renameFileName));
				    this.log.debug("rename file:" +  renameFileName + "-" + blrenamefile);
				    
				    int code = ftp.uploadFile(renameFileName, docName);
				    switch(code)
				    {
				    	case 100://�ɹ�
				    		log.debug("FTP Upload Code:100-"+renameFileName);
				    		break;
				    	case 400:
				    		//�쳣
				    		log.debug("FTP Upload Code:400-"+renameFileName);
				    		break;
				    	case 401:
				    		//��������ʧ��
				    		log.debug("FTP Upload Code:401-"+renameFileName);
				    		break;
				    }
				    
				    File sucfile = new File(renameFileName);
		    		if(sucfile.delete())
		    		{
		    			log.debug("�ļ�:[" + renameFileName + " ]ɾ���ɹ�");
		    		}
		    		
		    		File ackfile = new File(AckFileName);
				    ackfile.createNewFile();
		    		code = ftp.uploadFile(ackfile.getAbsolutePath(), docName);
		    		if(ackfile.delete())
		    		{
		    			log.debug("�ļ�:[" + AckFileName + " ]ɾ���ɹ�");
		    		}
				    	
				}
  				catch (Exception e)
  			    {
  			    	logStr = "�ַ�����: FTP�ɼ��쳣.";
  			    	this.log.error(logStr, e);
  			    }
  			    finally
  			    {
  			    	ftp.disconnect();
  			    }

			//}
			//else
			{
					//����ڱ���Ŀ¼
			}
				
		}
	}
}
