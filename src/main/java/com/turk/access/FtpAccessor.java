package com.turk.access;

//import access.special.EricssonWcdmaPerformanceAccessor;
import com.turk.Config.ConstDef;
import com.turk.framework.DataLifecycleMgr;
import com.turk.Config.SystemConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.turk.alarm.AlarmMgr;
import com.turk.collect.DownStructer;
import com.turk.collect.FTPTool;
import com.turk.distributor.DistributeTemplet;
import com.turk.distributor.TableItem;
import com.turk.task.IgnoresInfo;
import com.turk.task.IgnoresMgr;
import com.turk.task.TaskMgr;
import com.turk.templet.LineTempletP;
import com.turk.util.DeCompression;
//import com.turk.util.ExcelToCsvUtil;
import com.turk.util.Parsecmd;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * FTP 采集
 * @author Administrator
 *
 */
public class FtpAccessor extends AbstractAccessor
{
	//private static final byte MAX_TRY_TIMES = 5;
	//private int index = 0;

	private IgnoresMgr ignoresMgr = IgnoresMgr.getInstance();

	public boolean access()
    	throws Exception
	{
		boolean bSucceed = false;

	    int taskID = getTaskID();
	    FTPTool ftp = new FTPTool(this.taskInfo);
	    String logStr = this.name + ": logining ftp...";
	    this.log.debug(logStr);
	    this.taskInfo.log("start", logStr);
	    try
	    {
	    	boolean bOK = ftp.login(120*1000, 5);
	    	if (!bOK)
	    	{
	    		logStr = this.name + ": Several failed attempts to log in FTP:" + ftp;
	    		this.log.error(logStr);
	    		this.taskInfo.log("start", logStr);

		        TaskMgr.getInstance().newRegather(this.taskInfo, "", "Several failed attempts to log in FTP,re-collect");
		
		        AlarmMgr.getInstance().insert(taskID,(byte)2, "Several failed attempts to log in FTP", ftp.toString(), this.name, 10101);
		        return false;
	    	}
	    	logStr = this.name + ": login success.";
	    	this.log.debug(this.name + ": FTP login success.");
	    	this.taskInfo.log("start", logStr);
	    	ftp.disconnect();
	    	int parseType = this.taskInfo.getParseTmpType();
	    	
	    	
	    	
	    	
			
			//*******2011/05/03 TURK 分发模版
			DistributeTemplet disTemp
				= (DistributeTemplet)this.getTaskInfo().getDistributeTemplet();
			//***************************
      
			
			//if(templet.m_nTemplet.size() == 0)
			//{
			//	this.log.error(this.name + ": 数据接入失败,原因:数据源配置为空.");
			//	return false;
			//}

	    	String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
	    	String strRootTempPath = strCurrentPath + File.separatorChar + 
	    		taskID;

	    	String[] strNeedGatherFileNames = getDataSourceConfig().getDatas();

	    	Set<String> list = new HashSet<String>();
	    	for (String s : strNeedGatherFileNames)
	    	{
	    		try
	    		{
	    			String p = ConstDef.ParseFilePath(s, this.taskInfo.getLastCollectTime());
	    			
	    			list.addAll(ftp.listFTPDirs(p, this.taskInfo.getDevInfo().getIP(), 
	    					this.taskInfo.getDevPort(), this.taskInfo.getDevInfo().getHostUser(), 
	    					this.taskInfo.getDevInfo().getHostPwd(), this.taskInfo.getDevInfo().getEncode(), 
	    					this.taskInfo.getParserID()));
	    		}
	    		catch (Exception e)
	    		{
	    			this.log.error("Expand the directory wildcard exception",e);
	    		}
	    	}
	    	if (list.size() == 0)
	    	{
	    		this.log.warn("Expand the directory after the wildcard, the number of paths 0");
	    	}

	    	//获取需要下载的文件名
	    	strNeedGatherFileNames = (String[])list.toArray(new String[0]);

	    	Parsecmd parsecmd = new Parsecmd();
	    	long localtimecount = 0L;

	    	for (String gatherFileName : strNeedGatherFileNames)
	    	{
	    		//this.index += 1;
	    		if (Util.isNull(gatherFileName)) {
	    			continue;
	    		}
	    		String strSubFilePath = ConstDef.ParseFilePath(gatherFileName.trim(), this.taskInfo.getLastCollectTime());

	    		String strTempPath = ConstDef.CreateFolder(strRootTempPath, taskID, strSubFilePath);

	    		DownStructer dStruct = null;
	    		try
	    		{
	    			dStruct = ftp.downFile(strSubFilePath, strTempPath);
	    		}
	    		catch (Exception e)
	    		{
	    			TaskMgr.getInstance().newRegather(this.taskInfo, gatherFileName, "文件下载失败，异常信息为:" + 
	    					e.getMessage());
	    			
	    			continue;
	    		}

	    		if (dStruct.getSuc().size() == 0)
	    		{
	    			//若下载的文件都为空
	    			IgnoresInfo ignoresInfo = this.ignoresMgr.checkIgnore(this.taskInfo.getTaskID(), gatherFileName, this.taskInfo.getLastCollectTime());
	    			if (ignoresInfo == null)
	    			{
	    				TaskMgr.getInstance().newRegather(this.taskInfo, gatherFileName, "File does not exist");
	    			}
	    			else
	    			{
	    				this.log.warn(this.name + " " + gatherFileName + 
	    						" does not exist,but [utl_conf_ignores] Ignore this path set(" + 
	    						ignoresInfo + "),does not re-collect.");
	    			}
	    		}
	    		else
	    		{
	    			
	    			if (dStruct.getFail().size() > 0)
	    			{
	    				for (String fFile : dStruct.getFail())
	    				{
	    					TaskMgr.getInstance().newRegather(this.taskInfo, fFile, "ftp文件长度为0");
	    					IgnoresInfo ignoresInfo = this.ignoresMgr.checkIgnore(this.taskInfo.getTaskID(), fFile, this.taskInfo.getLastCollectTime());
	    					if (ignoresInfo == null)
	    						continue;
	    					this.log.warn(this.name + " " + fFile + 
	    							",  [utl_conf_ignores] Ignore this path set(" + 
	    							ignoresInfo + "),but it's exist,no longer ignore the path.");
	    					ignoresInfo.setNotUsed();
	    				}
	    			}

	    			List<String> arrfileList = new ArrayList<String>();

	    			for (String strFileName : dStruct.getSuc())
	    			{
	    				if (Util.isNull(strFileName)) {
	    					continue;
	    				}
	    				IgnoresInfo ignoresInfo = this.ignoresMgr.checkIgnore(this.taskInfo.getTaskID(), strFileName, this.taskInfo.getLastCollectTime());
	    				if (ignoresInfo != null)
	    				{
	    					this.log.warn(this.name + " " + strFileName + 
	    							",  [utl_conf_ignores]  Ignore this path set(" + 
	    							ignoresInfo + "),but it's exist,no longer ignore the path.");
	    					ignoresInfo.setNotUsed();
	    				}

	    				if (Util.isZipFile(strFileName))
	    				{
	    					try
	    					{
	    						arrfileList = DeCompression.decompress(taskID, this.taskInfo.getParseTemplet(), strFileName, this.taskInfo.getLastCollectTime(), this.taskInfo.getPeriod());
	    					}
	    					catch (Exception e)
	    					{
	    						logStr = this.name + ": File decompression failed " + strFileName + 
	    						" . cause:";
	    						this.log.error(logStr, e);
	    						this.taskInfo.log("start", logStr, e);

	    						TaskMgr.getInstance().newRegather(this.taskInfo, gatherFileName, "解压文件时异常,异常信息为:" + 
	    								e.getMessage());
	    					}

	    				}
	    				else
	    				{
	    					arrfileList.add(strFileName);
	    				}

	    			}

//	    			List<String> xlsList = new ArrayList<String>();
//	    			List<String> totalCsv = new ArrayList<String>();
//	    			for (String oneFile : arrfileList)
//	    			{
//	    				if ((!oneFile.endsWith(".xls")) || 
//	    						(this.taskInfo.getParserID() == 9001))
//	    					continue;
//	    				try
//	    				{
//	    					List<String> csvFiles = new ExcelToCsvUtil(oneFile, this.taskInfo).toCsv();
//	    					((List<String>)xlsList).add(oneFile);
//	    					totalCsv.addAll(csvFiles);
//	    				}
//	    				catch (Exception e)
//	    				{
//	    					this.errorlog.error(this.name + " converting excel exception: " + oneFile, e);
//	    				}
//	    			}
//
//	    			for (String s : xlsList)
//	    			{
//	    				arrfileList.remove(s);
//	    			}
//	    			arrfileList.addAll(totalCsv);
//	    			((List<String>)xlsList).clear();
//	    			totalCsv.clear();
//	    			xlsList = null;
//	    			totalCsv = null;

	    			if ((arrfileList == null) || (arrfileList.size() == 0))
	    			{
	    				continue;
	    			}
	    			for (int j = 0; j < arrfileList.size(); j++)
	    			{
	    				String strTempFileName = (String)arrfileList.get(j);
	    				Date dataTime = this.taskInfo.getLastCollectTime();

	    				DataLifecycleMgr.getInstance().doFileTimestamp(strTempFileName, dataTime);
	    			}

	    			String strCmd = this.taskInfo.getShellCmdPrepare();
	    			if (Util.isNotNull(strCmd))
	    			{
	    				boolean b = Parsecmd.ExecShellCmdByFtp1(strCmd, this.taskInfo.getLastCollectTime());
	    				if (!b)
	    				{
	    					logStr = this.name + ": ftp command failed execution. " + strCmd;
	    					this.errorlog.error(logStr);
	    					this.taskInfo.log("start", logStr);
	    				}
	    			}

	    			logStr = this.name + ": pares type=" + parseType;
	    			this.log.debug(logStr);
	    			this.taskInfo.log("开始", logStr);

	    			this.parser.setDsConfigName(gatherFileName);

	    			for (int j = 0; j < arrfileList.size(); j++)
	    			{
	    				String strTempFileName = (String)arrfileList.get(j);

	    				if ((this.taskInfo.getParseTmpType() == 24) && 
	    						(strTempFileName.endsWith(".fix")))
	    				{
	    					continue;
	    				}

	    				logStr = this.name + ": The current file to be parsed:" + strTempFileName;
	    				this.log.debug(logStr);
	    				this.taskInfo.log("parse", logStr);
	    				this.parser.setFileName(strTempFileName);
	    				try
	    				{
	    					this.parser.parseData();
	    				}
	    				catch (Exception e)
	    				{
	    					logStr = this.name + ": parse fail(" + strTempFileName + "),caues:";
	    					this.errorlog.error(logStr, e);
	    					this.taskInfo.log("parse", logStr, e);
	    					continue;
	    				}
	    				
	    				
	    				
	    		    	
	    				//******START**TURK 2011/05/03 
	    				//Modify By Turk 2011/10/12 修改为多种方式分发文件
						//******解析完成后，匹配解析的文件，是否需要复制文件到汇总目录下[路测汇总使用]
	    				//解析模版 //只有在行解析的情况下做如下处理
	    				if(this.getTaskInfo().getParseTmpType() == 1)
	    				{
		    		    	LineTempletP templet = (LineTempletP)this.getTaskInfo().getParseTemplet();
							for(int ii = 0;ii < disTemp.tableTemplets.size(); ii++)
							{
								if(templet.m_nTemplet.size()<=ii)
									break;
								
								LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(ii);
								DistributeTemplet.TableTemplet temp = (DistributeTemplet.TableTemplet)disTemp.tableTemplets.get(ii);
							
								if(subTemp == null || temp == null)
									continue;
								
								if(temp.isCalu && !temp.bakDirectory.isEmpty())
								{
									if(temp.bakDirectory.contains("ftp://"))
									{//备份共享到远端FTP服务器
										String[] strArray1 = temp.bakDirectory.split("//",-1);
										if(strArray1.length >= 2)
										{
											String IP = strArray1[1].substring(0,strArray1[1].indexOf("/"));
											String path = strArray1[1].substring(strArray1[1].indexOf("/"));
											//目录做替换
											path = path.replace("%CITYID", 
													String.valueOf(this.getTaskInfo().getDevInfo().getCityID())); //城市编号
											//SimpleDateFormat ft1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
											Calendar calendar = Calendar.getInstance(); 
											calendar.setTime(this.getTaskInfo().getLastCollectTime());
											//时间、日期
											String YY = String.format("%04d",calendar.get(Calendar.YEAR));
											String MM = String.format("%02d",calendar.get(Calendar.MONTH)+1);
											String DD = String.format("%02d",calendar.get(Calendar.DATE));
											String HH = String.format("%02d",calendar.get(Calendar.HOUR_OF_DAY));
											String MI = String.format("%02d",calendar.get(Calendar.MINUTE));
											path = path.replace("%YY",YY).replace("%MM", MM)
												.replace("%DD", DD).replace("%HH", HH).replace("%MI", MI);
											
											path = path.replace("%DEVICENAME", 
													this.getTaskInfo().getDevInfo().getDeviceName()); //设备名称
											
											//上传FTP
											
											String ftpIP = IP;
											String ftpuser = SystemConfig.getInstance().getGpUser();
											String ftppwd = SystemConfig.getInstance().getGpPwd();
											FTPTool ftpupload = new FTPTool(ftpIP,21,ftpuser,ftppwd);
											
											//已当前采集任务描述作为采集目录
											String docName = path;
											ftp.setKeyID(String.valueOf(this.getTaskID()));
											
											
											logStr = "共享数据：开始FTP登陆.";
											this.log.debug(logStr);
											try
											{
												bOK = ftpupload.login(30000, 5);
												if (!bOK)
											 	{
													logStr = "共享数据: FTP多次尝试登陆失败:" + ftp;
													this.errorlog.error(logStr);
													return false;
											 	}
											    logStr = "共享数据: FTP登陆成功.";
											    this.log.debug("共享数据: FTP登陆成功.");
											    //原始文件直接共享
											    String fileName = strTempFileName;
												fileName = fileName.replace(" ", "_");
											   
											    int code = ftpupload.uploadFile(fileName, docName);
											    switch(code)
											    {
											    	case 100://成功
											    		File sucfile = new File(fileName);
											    		if(sucfile.delete())
											    		{
											    			log.debug("文件:[" + fileName + " ]删除成功");
											    		}
											    		break;
											    	case 400:
											    		//异常
											    		break;
											    	case 401:
											    		//三次重试失败
											    		break;
											    }
											    	
											}
							  				catch (Exception e)
							  			    {
							  			    	logStr = "共享数据: FTP采集异常.";
							  			    	this.errorlog.error(logStr, e);
							  			    }
							  			    finally
							  			    {
							  			    	ftpupload.disconnect();
							  			    }
	
										}
										
									}
									else
									{//备份在本地服务器
										String strFileName = "";
										//检查文件名是否有后缀，如果没有后缀，则需要按照分发表去备份文件
										if(subTemp.m_strFileName.indexOf(".") > 0)
										{
											
											strFileName = ConstDef.ParseFilePath(subTemp.m_strFileName, this.taskInfo.getLastCollectTime());
											strFileName = strFileName.substring(strFileName.lastIndexOf(File.separator) + 1);
											if(strTempFileName.toUpperCase().contains(strFileName.toUpperCase()))
											{
												//创建文件夹
												String strcaluFile = ConstDef.CreateFolder(SystemConfig.getInstance().getCurrentPath()
														+ File.separator + temp.bakDirectory + File.separator
													, String.format("%s", taskInfo.getDevInfo().getDevID()), gatherFileName);
												//移动文件
												String fileName = strTempFileName.substring(strTempFileName.lastIndexOf(File.separator) + 1);
												fileName = fileName.replace(" ", "_");
												boolean copyResult = Util.FileCopy(strTempFileName, strcaluFile + File.separator + fileName);
												log.debug(String.format("copy files:%s->%s --%s"
														,strTempFileName,strcaluFile + File.separator + fileName,copyResult));
												break;
											}
										}
										else if(subTemp.m_strFileName.equals(""))
										{
											//创建文件夹 -- 汇总目录下创建设备编号的文件夹
											String strcaluFile = ConstDef.CreateFolder(SystemConfig.getInstance().getCurrentPath()
													+ File.separator + temp.bakDirectory + File.separator
												, String.format("%s", taskInfo.getDevInfo().getDeviceName()));
											//移动文件
											String fileName = strTempFileName.substring(strTempFileName.lastIndexOf(File.separator) + 1);
											fileName = fileName.replace(" ", "_");
											boolean copyResult = Util.FileCopy(strTempFileName, strcaluFile + File.separator + fileName);
											log.debug(String.format("copy files:%s->%s --%s"
													,strTempFileName,strcaluFile + File.separator + fileName,copyResult));
											break;
										}
										else
										{
											//File.
											strFileName = temp.tableName+"_"+ Util.getDateString_yyyyMMddHHmmss(this.taskInfo.getLastCollectTime()) + ".txt";
											//temp.
											TableItem tableItem = disTemp.tableItems.get(ii);
										
											String strcaluFile = ConstDef.CreateFolder(SystemConfig.getInstance().getCurrentPath()
													+ File.separator + temp.bakDirectory + File.separator
												, String.format("%s", taskInfo.getDevInfo().getDeviceName()));
											//移动文件
											strTempFileName = SystemConfig.getInstance().getCurrentPath()+File.separator+ tableItem.fileName + ".txt";
											strFileName = strFileName.replace(" ", "_");
											boolean copyResult = Util.FileCopy(strTempFileName, strcaluFile + File.separator + strFileName);
											log.debug(String.format("coyp files:%s->%s --%s"
													,strTempFileName,strcaluFile + File.separator + strFileName,copyResult));
										//break;
										}
									}
								}
							}
							
							
						}
						//******END***********************************************************

	    				if ((!DataLifecycleMgr.getInstance().isEnable()) && 
	    						(DataLifecycleMgr.getInstance().isDeleteWhenOff()))
	    				{
	    					File f = new File(strTempFileName);
	    					f.delete();
	    				}
	    				logStr = this.name + ": " + strTempFileName + " parse complate.";
	    				this.log.info(logStr);
	    				this.taskInfo.log("parse", logStr);
	    			}
	    		}
	    	}
	    	logStr = this.name + ": Completion of the distribution process data parse,Time-consuming：" + localtimecount ;//+ " MR数:" + 
	    	//mrsumcount + "," + "定位: " + this.taskInfo.m_nAllRecordCount;
	    	this.log.info(logStr);
	    	this.taskInfo.log("parse", logStr);

	    	parsecmd.comitmovefiles();

	    	bSucceed = true;
	    }
	    catch (Exception e)
	    {
	    	logStr = this.name + ": FTP collect exception.";
	    	this.errorlog.error(logStr, e);
	    	this.taskInfo.log("start", logStr, e);

	    	AlarmMgr.getInstance().insert(taskID,(byte)2, "FTP collect exception", this.name, e.getMessage(), 10102);
	    }
	    finally
	    {
	    	ftp.disconnect();
	    }
	    //DataLogMgr.getInstance().FtpLogCommint();
	    return bSucceed;
	}

	public void configure()
		throws Exception
		{
		}


	public boolean doBeforeAccess()
	    throws Exception
	{
		
		return true;
	}
	
	public boolean doAfterAccess()
		throws Exception
	{
		String strShellCmdFinish = this.taskInfo.getShellCmdFinish();
		if (Util.isNotNull(strShellCmdFinish))
		{
			Parsecmd.ExecShellCmdByFtp(strShellCmdFinish, this.taskInfo.getLastCollectTime());
		}

		return true;
	}

	@Override
	public String info() {
		// TODO Auto-generated method stub
		String taskinfo = String.format("ID:%s Time:%s Name:%s StartTime:%s", 
				this.taskInfo.getTaskID(),this.taskInfo.getLastCollectTime(),
				this.taskInfo.getDescribe(),this.getBeginExceuteTime());
		return taskinfo;
	}

	@Override
	protected boolean needExecuteImmediate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Task taskCore() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean useDb() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stopTask() {
		// TODO Auto-generated method stub
		
	}
}