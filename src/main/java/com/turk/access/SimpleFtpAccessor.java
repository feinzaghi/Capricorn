package com.turk.access;

import com.turk.framework.DataLifecycleMgr;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.turk.alarm.AlarmMgr;
import com.turk.collect.DownStructer;
import com.turk.collect.FTPTool;
import com.turk.config.ConstDef;
import com.turk.config.SystemConfig;
import com.turk.task.RegatherObjInfo;
import com.turk.task.TaskMgr;
import com.turk.util.Parsecmd;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * 简单FTP采集
 * @author Administrator
 *
 */
public class SimpleFtpAccessor extends AbstractAccessor
{
	public boolean access()
		throws Exception
    {
		boolean bSucceed = false;

		int taskID = getTaskID();
		FTPTool ftp = new FTPTool(this.taskInfo);
		String logStr = this.name + ": 开始FTP登陆.";
		this.log.debug(logStr);
		this.taskInfo.log("开始", logStr);
		try
		{
			boolean bOK = ftp.login(30000, 5);
			if (!bOK)
			{
				logStr = this.name + ": FTP多次尝试登陆失败:" + ftp;
				this.log.error(logStr);
				this.taskInfo.log("开始", logStr);

				TaskMgr.getInstance().newRegather(this.taskInfo, "", "多次登陆失败，全部补采");

				AlarmMgr.getInstance().insert(taskID,(byte)2, "FTP多次尝试登陆失败", ftp.toString(), this.name, 10101);

				return false;
			}
			logStr = this.name + ": FTP登陆成功.";
			this.log.debug(this.name + ": FTP登陆成功.");
			this.taskInfo.log("开始", logStr);

			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strRootTempPath = strCurrentPath + File.separatorChar + taskID;

			String[] strNeedGatherFileNames = getDataSourceConfig().getDatas();

			List<String> list = new ArrayList<String>();
			for (String s : strNeedGatherFileNames)
			{
				try
				{
					list.addAll(Util.listFTPDirs(ConstDef.ParseFilePath(s, this.taskInfo.getLastCollectTime()), this.taskInfo.getDevInfo().getIP(), this.taskInfo.getDevPort(), this.taskInfo.getDevInfo().getHostUser(), this.taskInfo.getDevInfo().getHostPwd(), this.taskInfo.getDevInfo().getEncode(), this.taskInfo.getParserID()));
				}
				catch (Exception e)
				{
					this.log.error("展开目录通配符时异常");
					throw e;
				}
			}
			if (list.size() == 0)
			{
				this.log.warn("展开目录通配符后，路径条数为0");
			}
			else
			{
				this.log.debug("展开目录 - " + list);
			}

			List<String> tmpList = new ArrayList<String>(list);
			list.clear();
			String str = getDataSourceConfig().getDatas()[0];
			String[] sp = str.split("/");
			int wIndex = -1;
			for (int i = 0; i < sp.length; i++)
			{
				if (!sp[i].equals("*"))
					continue;
				wIndex = i;
				break;
			}

			for (String s : tmpList)
			{
				if (wIndex == -1)
				{
					list.add(s);
				}
				else {
					StringBuilder onePath = new StringBuilder();
					sp = s.split("/");
					for (int i = 0; i < sp.length; i++)
					{
						if (i == wIndex)
						{
							onePath.append("!").append(sp[i]).append("{").append(this.taskInfo.getTaskID()).append("}!").append("/");
						}
						else
							onePath.append(sp[i]).append("/");
					}
					onePath.delete(onePath.length() - 1, onePath.length());
					list.add(onePath.toString());
				}
			}
			strNeedGatherFileNames = (String[])list.toArray(new String[0]);

			Parsecmd parsecmd = new Parsecmd();
			String[] arrayOfString2 = strNeedGatherFileNames;
     
			for (int i = 0; i < arrayOfString2.length; i++) 
			{ 
				String gatherFileName = arrayOfString2[i];

				
				if (Util.isNull(gatherFileName)) {
					continue;
				}
				String strSubFilePath = ConstDef.ParseFilePath(gatherFileName.trim(), this.taskInfo.getLastCollectTime());
    	  
				if (this.taskInfo.getParseTmpType() == 24)
				{
    			  strSubFilePath = Util.checkEnsurePos(strSubFilePath);
				}
				
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
					TaskMgr.getInstance().newRegather(this.taskInfo, gatherFileName, "文件不存在");
				}
				else
				{
					List<String> arrfileList = new ArrayList<String>();

					for (String strFileName : dStruct.getSuc())
					{
						if (Util.isNull(strFileName))
						{
							continue;
						}
	
						arrfileList.add(strFileName);
					}

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
							logStr = this.name + ": ftp执行命令失败. " + strCmd;
							this.log.error(logStr);
							this.taskInfo.log("开始", logStr);
						}
					}

					this.parser.setDsConfigName(gatherFileName);
				}
			}

			parsecmd.comitmovefiles();

			bSucceed = true;
		}
		catch (Exception e)
		{
			logStr = this.name + ": FTP下载异常.";
			this.errorlog.error(logStr, e);
			this.taskInfo.log("开始", logStr, e);

			AlarmMgr.getInstance().insert(taskID, (byte)2, "FTP下载异常", this.name, e.getMessage(), 10102);
		}
		finally
		{
			ftp.disconnect();
		}
		
		return bSucceed;
    }

	public void configure()
	{
	}

	public void doSqlLoad()
	{
	}

	public void dispose(long lastCollectTime)
	{
		this.runFlag = false;
		this.taskInfo.setUsed(false);

		String logStr = this.name + ": remove from active-task-map. " + 
			this.strLastGatherTime;
		this.log.debug(logStr);
		this.taskInfo.log("结束", logStr);
		TaskMgr.getInstance().delActiveTask(this.taskInfo.getKeyID(), this.taskInfo instanceof RegatherObjInfo);

		TaskMgr.getInstance().commitRegather(this.taskInfo, lastCollectTime);
	}

	@Override
	public String info() {
		// TODO Auto-generated method stub
		return null;
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