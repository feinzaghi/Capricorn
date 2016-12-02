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
 * 文件方式分发
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
	 * 文件分发到FTP
	 * @param tableIndex
	 * @param tempFile
	 */
	public void BuildFileUploadFtp(int tableIndex, String tempFile)
	{
		String logStr = "";
		//得到当前表的分发模版
		DistributeTemplet.TableTemplet TableInfo 
			= (DistributeTemplet.TableTemplet)this.disTmp.tableTemplets.get(Integer.valueOf(tableIndex));
		
		Map<Integer, TableItem> tableItems = this.disTmp.tableItems;
		TableItem tableItem = tableItems.get(Integer.valueOf(tableIndex));
		
		//if(!TableInfo.UploadPath.isEmpty())
		//{
			String uploadPath = TableInfo.UploadPath;
			//将解析的文件上传到指定目录
			//
			
			//if(uploadPath.toLowerCase().contains("ftp"))
			{ //FTP 地址，启动FTP上传
				String ftpIP = collectInfo.getInDBServerConfig().getInDBServer();
				String ftpuser = collectInfo.getInDBServerConfig().getInDBUser();
				String ftppwd = collectInfo.getInDBServerConfig().getInDBPassword();
				FTPTool ftp = new FTPTool(ftpIP,21,ftpuser,ftppwd);
				
				//已当前采集任务描述作为采集目录
				String docName = uploadPath;
				ftp.setKeyID(String.valueOf(collectInfo.getTaskID()));
				
				
				logStr = "分发数据：开始FTP登陆.";
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
				    
				  //判断文件大小，如若为0，不上传
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
						logStr = "分发数据: FTP多次尝试登陆失败:" + ftp;
						this.log.error(logStr);
						return;
				 	}
				    logStr = "分发数据: FTP登陆成功.";
				    this.log.debug(logStr);
				    
				    int code = ftp.uploadFile(renameFileName, docName);
				    switch(code)
				    {
				    	case 100://成功
				    		log.debug("FTP Upload Code:100-"+renameFileName);
				    		break;
				    	case 400:
				    		//异常
				    		log.debug("FTP Upload Code:400-"+renameFileName);
				    		break;
				    	case 401:
				    		//三次重试失败
				    		log.debug("FTP Upload Code:401-"+renameFileName);
				    		break;
				    }
				    
				    File sucfile = new File(renameFileName);
		    		if(sucfile.delete())
		    		{
		    			log.debug("文件:[" + renameFileName + " ]删除成功");
		    		}
		    		
		    		File ackfile = new File(AckFileName);
				    ackfile.createNewFile();
		    		code = ftp.uploadFile(ackfile.getAbsolutePath(), docName);
		    		if(ackfile.delete())
		    		{
		    			log.debug("文件:[" + AckFileName + " ]删除成功");
		    		}
				    	
				}
  				catch (Exception e)
  			    {
  			    	logStr = "分发数据: FTP采集异常.";
  			    	this.log.error(logStr, e);
  			    }
  			    finally
  			    {
  			    	ftp.disconnect();
  			    }

			//}
			//else
			{
					//存放于本地目录
			}
				
		}
	}
	
	
	/**
	 * 文件分发到FTP
	 * @param tableIndex
	 * @param tempFile
	 */
	private void BuildFileUploadFtp(String FileName,int tableIndex, String tempFile)
	{
		String logStr = "";
		//得到当前表的分发模版
		DistributeTemplet.TableTemplet TableInfo 
			= (DistributeTemplet.TableTemplet)this.disTmp.tableTemplets.get(Integer.valueOf(tableIndex));
		
		//Map<Integer, TableItem> tableItems = this.disTmp.tableItems;
		//TableItem tableItem = tableItems.get(Integer.valueOf(tableIndex));
		
		//if(!TableInfo.UploadPath.isEmpty())
		//{
			String uploadPath = TableInfo.UploadPath;
			//将解析的文件上传到指定目录
			//
			
			//if(uploadPath.toLowerCase().contains("ftp"))
			{ //FTP 地址，启动FTP上传
				String ftpIP = collectInfo.getInDBServerConfig().getInDBServer();
				String ftpuser = collectInfo.getInDBServerConfig().getInDBUser();
				String ftppwd = collectInfo.getInDBServerConfig().getInDBPassword();
				FTPTool ftp = new FTPTool(ftpIP,21,ftpuser,ftppwd);
				
				//已当前采集任务描述作为采集目录
				String docName = uploadPath;
				ftp.setKeyID(String.valueOf(collectInfo.getTaskID()));
				
				
				logStr = "分发数据：开始FTP登陆.";
				this.log.debug(logStr);
				try
				{
					boolean bOK = ftp.login(30000, 5);
					if (!bOK)
				 	{
						logStr = "分发数据: FTP多次尝试登陆失败:" + ftp;
						this.log.error(logStr);
						return;
				 	}
				    logStr = "分发数据: FTP登陆成功.";
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
				    	case 100://成功
				    		log.debug("FTP Upload Code:100-"+renameFileName);
				    		break;
				    	case 400:
				    		//异常
				    		log.debug("FTP Upload Code:400-"+renameFileName);
				    		break;
				    	case 401:
				    		//三次重试失败
				    		log.debug("FTP Upload Code:401-"+renameFileName);
				    		break;
				    }
				    
				    File sucfile = new File(renameFileName);
		    		if(sucfile.delete())
		    		{
		    			log.debug("文件:[" + renameFileName + " ]删除成功");
		    		}
		    		
		    		File ackfile = new File(AckFileName);
				    ackfile.createNewFile();
		    		code = ftp.uploadFile(ackfile.getAbsolutePath(), docName);
		    		if(ackfile.delete())
		    		{
		    			log.debug("文件:[" + AckFileName + " ]删除成功");
		    		}
				    	
				}
  				catch (Exception e)
  			    {
  			    	logStr = "分发数据: FTP采集异常.";
  			    	this.log.error(logStr, e);
  			    }
  			    finally
  			    {
  			    	ftp.disconnect();
  			    }

			//}
			//else
			{
					//存放于本地目录
			}
				
		}
	}
}
