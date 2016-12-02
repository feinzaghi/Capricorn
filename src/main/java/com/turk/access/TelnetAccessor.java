package com.turk.access;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Set;

import com.turk.collect.ProtocolTelnet;
import com.turk.config.ConstDef;
import com.turk.config.SystemConfig;
import com.turk.parser.LineParser;
import com.turk.util.CommonDB;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * Telnet访问
 * @author Administrator
 *
 */
public class TelnetAccessor extends AbstractAccessor
{
	private boolean bRunFlag = true;

	public boolean access()
		throws Exception
    {
		String logStr = null;

		boolean result = false;

		ProtocolTelnet telnet = new ProtocolTelnet();

		byte[] bufRecv = new byte[1048576];

		int taskID = getTaskID();
		String des = this.taskInfo.getDescribe();
		int gatherTimeout = this.taskInfo.getCollectTimeOut();
		int collectPeriod = this.taskInfo.getPeriod();
		Timestamp lastGatherTime = this.taskInfo.getLastCollectTime();
		int lastCollectPos = this.taskInfo.getLastCollectPos();

		logStr = this.name + ": 准备 Telnet 登陆.";
		this.log.debug(logStr);
		this.taskInfo.log("开始", logStr);
    
		String strHostIP = this.taskInfo.getDevInfo().getIP();
		int nHostPort = this.taskInfo.getDevPort();
		String strUser = this.taskInfo.getDevInfo().getHostUser();
		String strPassword = this.taskInfo.getDevInfo().getHostPwd();
		String strHostSign = this.taskInfo.getDevInfo().getHostSign();
		try
		{
			if (!telnet.Login(strHostIP, nHostPort, strUser, strPassword, strHostSign, "ANSI", gatherTimeout))
			{
				String strError = String.format("Telnet 登陆失败. Host=%s Port=%d(%s)", new Object[] { strHostIP, Integer.valueOf(nHostPort), des });
				logStr = this.name + ": " + strError;
				this.log.error(logStr);
				this.taskInfo.log("开始", logStr);
				return result;
			}
			logStr = this.name + "Telnet 登陆成功.";
			this.log.debug(logStr);
			this.taskInfo.log("开始", logStr);

			String strProxyHostIP = this.taskInfo.getProxyDevInfo().getIP();
			if (Util.isNotNull(strProxyHostIP))
			{
				strHostSign = this.taskInfo.getProxyDevInfo().getHostSign();

				int nProxyPort = this.taskInfo.getProxyDevPort();
				String strProxyUser = this.taskInfo.getProxyDevInfo().getHostUser();
				String strProxyPassword = this.taskInfo.getProxyDevInfo().getHostPwd();

				if (!telnet.ProxyLogin(strProxyHostIP, nProxyPort, strProxyUser, strProxyPassword, strHostSign, "ANSI"))
				{
					String strError = String.format("Proxy Telnet 登陆失败: Host=%s Port=%d(%s)", new Object[] { strProxyHostIP, Integer.valueOf(nProxyPort), des });
					logStr = this.name + ": " + strError;
					this.log.error(logStr);
					this.taskInfo.log("开始", logStr);
					return result;
				}
				logStr = this.name + ": Proxy Telnet 登陆成功.";
				this.log.debug(logStr);
				this.taskInfo.log("开始", logStr);
			}

			String strShellCmdPrepare = this.taskInfo.getShellCmdPrepare();
			if (Util.isNotNull(strShellCmdPrepare))
			{
				if (!telnet.ExecuteShell(strShellCmdPrepare, strHostSign, this.taskInfo.getShellTimeout()))
				{
					String strError = "error when execute script. " + 
						strShellCmdPrepare;
					logStr = this.name + ": " + strError;
					this.log.error(logStr);
					this.taskInfo.log("开始", logStr);
					return result;
				}
			}

			String strTimeFile1 = ConstDef.ParseFilePath(this.taskInfo.getCollectPath(), lastGatherTime);

			String[] strNeedGatherFileNames = (String[])null;

			Set<String> list = new HashSet<String>();
			String[] filepaths = strTimeFile1.split(";");
			if (filepaths != null)
			{
				for (String f : filepaths)
				{
					if (Util.isNull(f))
						continue;
					list.add(f);
				}
			}
			if (list.size() == 0)
			{
				this.log.warn("数据源路径，路径条数为0");
			}

			strNeedGatherFileNames = (String[])list.toArray(new String[0]);

			if (strNeedGatherFileNames == null)
			{
				this.log.debug(this.name + "请检查数据源");
			}
			for (String strTimeFile : strNeedGatherFileNames)
			{
				logStr = this.name + ": 采集的文件:" + strTimeFile;
				this.log.debug(logStr);
				this.taskInfo.log("开始", logStr);

				boolean bFound = telnet.findFile(strTimeFile, this.taskInfo.getDevInfo().getHostSign(), gatherTimeout);

				if (!bFound)
				{
					logStr = this.name + ": " + strTimeFile + " 不存在.";
					this.log.info(logStr);
					this.taskInfo.log("开始", logStr);
					return result;
				}

				String strCmd = null;
				if (collectPeriod == 1)
				{
					strCmd = String.format("tail +%dcf %s", new Object[] { Integer.valueOf(lastCollectPos), strTimeFile });
				}
				else
				{
					strCmd = String.format("tail +%dc %s", new Object[] { Integer.valueOf(lastCollectPos), strTimeFile });
				}

				logStr = this.name + ": 开始发送命令:" + strCmd;
				this.log.debug(logStr);
				this.taskInfo.log("开始", logStr);

				boolean bFlag = telnet.sendCmd(strCmd);

				if (!bFlag)
				{
					logStr = this.name + ": 命令发送失败,任务将结束. " + strCmd;
					this.log.error(logStr);
					this.taskInfo.log("开始", logStr);
					return result;
				}
				logStr = this.name + ": 命令发送成功:" + strCmd;
				this.log.debug(logStr);
				this.taskInfo.log("开始", logStr);
				try
				{
					Thread.sleep(2000L);
				}
				catch (InterruptedException localInterruptedException)
				{
				}

				BufferedWriter bw = null;
				File file = null;

				if (this.taskInfo.getParserID() == 11)
				{
					String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
					String strRootTempPath = strCurrentPath + 
						File.separatorChar + taskID;

					String strTempPath = ConstDef.CreateFolder(strRootTempPath, taskID, strTimeFile);

					int index = strTimeFile.lastIndexOf("/");
					String fname = null;
					if (index != -1)
					{
						fname = strTimeFile.substring(index + 1);
					}
					else
					{
						fname = strTimeFile;
					}

					file = new File(strTempPath + File.separator + fname);

					this.log.debug(this.name + "本地文件名" + file.getAbsolutePath());

					if (file.exists())
					{
						file.delete();
					}
					else {
						file.createNewFile();
					}
					this.parser.setFileName(file.getAbsolutePath());

					bw = new BufferedWriter(new FileWriter(file, true));
				}

				int nRet = 0;
				int nRunCount = 0;

				while (this.bRunFlag)
				{
					nRunCount++;

					nRet = telnet.readData(bufRecv);

					if (nRet != -1)
					{
						if (this.taskInfo.getParserID() == 1)
						{
							parse(Util.bytesToChars(bufRecv), nRet);
						}
						else if (bw != null)
						{
//							this.parser.buildAlData(Util.bytesToChars(bufRecv), nRet, bw);
						}

						this.taskInfo.setCollectTimePos(nRet);
						if (nRunCount % 50 == 0) {
							CommonDB.LastImportTimePos(taskID, lastGatherTime, this.taskInfo.getLastCollectPos());
						}
					}
					else
					{
						StringBuffer buf = new StringBuffer();
						buf.append("\n**FILEEND**\n");
						
						if (this.taskInfo.getParserID() == 1)
						{
							parse(buf.toString().toCharArray(), buf.toString().length());
						}
						else if (bw != null)
						{
//							((CV1ASCII)this.parser).buildAlData(buf.toString().toCharArray(), buf.toString().length(), bw);
						}

						logStr = this.name + ": " + des + ":" + lastGatherTime + 
							" import finish.";
						this.log.debug(logStr);
						this.taskInfo.log("开始", logStr);
						break;
					}
				}

				if (bw != null)
				{
					bw.flush();
					bw.close();
				}
				if (this.taskInfo.getParserID() == 11)
				{
//					((CV1ASCII)this.parser).parseData();
				}

				logStr = this.name + ": Telnet: read data ok.";
				this.log.debug(logStr);
				this.taskInfo.log("开始", logStr);
				result = true;
			}
		}
		catch (Exception e)
		{
			result = false;
			logStr = this.name + ": Telnet采集异常. Cause:";
			this.errorlog.error(logStr, e);
			this.taskInfo.log("开始", logStr, e);
		}
		finally
		{
			telnet.Close();
		}

		return result;
    }

	public void configure()
		throws Exception
    {
    }

	public void parse(char[] chData, int len)
    	throws Exception
    {
		((LineParser)this.parser).BuildData(chData, len);
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