package com.turk.access;

import com.turk.Config.ConstDef;

import com.turk.framework.DataLifecycleMgr;
import com.turk.Config.SystemConfig;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;

import com.turk.util.DeCompression;
import com.turk.util.ExternalCmd;
import com.turk.util.Parsecmd;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * 本地文件采集
 * @author Administrator
 *
 */
public class LocalFileAccessor extends AbstractAccessor
{
	public boolean access()
		throws Exception
    {
		int taskID = getTaskID();

		String[] strSubPath = getDataSourceConfig().getDatas();
		String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
		String strRootTempPath = strCurrentPath + File.separatorChar + taskID;

		ArrayList<String> arrfileList = new ArrayList<String>();
		for (String subPath : strSubPath)
		{
			if (Util.isNull(subPath)) {
				continue;
			}
			String strSubFilePath = ConstDef.ParseFilePath(subPath.trim(), this.taskInfo.getLastCollectTime());

			String strTempPath = ConstDef.CreateFolder(strRootTempPath, taskID, strSubFilePath);
			File oldf = new File(strSubFilePath);
			if ((!oldf.exists()) || (!oldf.isFile()))
			{
				this.log.debug(this.name + "：文件：" + strSubFilePath + "不存在");
			}
			else
			{
				strTempPath = oldf.getPath();
				String[] strFileName = strTempPath.split(";");

				for (int j = 0; j < strFileName.length; j++)
				{
					if (strFileName[j] == null)
					{
						continue;
					}
					if (Util.isZipFile(strFileName[j]))
					{
						arrfileList = DeCompression.decompress(taskID, this.taskInfo.getParseTemplet(), strFileName[j], this.taskInfo.getLastCollectTime(), this.taskInfo.getPeriod());
					}
					else arrfileList.add(strFileName[j]);

				}

				String strShellCmdPrepare = this.taskInfo.getShellCmdPrepare();
				if (!Util.isNotNull(strShellCmdPrepare))
					continue;
				boolean bSuccess = Parsecmd.ExecShellCmdByFtp(strShellCmdPrepare, this.taskInfo.getLastCollectTime());
				if (!bSuccess) {
					this.errorlog.error(this.name + "：文件采集执行ShellCmdPrepare命令失败.");
				}
			}
		}
		if ((arrfileList == null) || (arrfileList.isEmpty()))
			return false;
		this.log.debug(this.name + "：解压缩文件个数:" + arrfileList.size());
		this.log.debug(this.name + ": 解析类型=" + this.taskInfo.getParseTmpType());

		Date dataTime = this.taskInfo.getLastCollectTime();

		for (String fileName : arrfileList)
		{
			this.parser.setDsConfigName(fileName);
			DataLifecycleMgr.getInstance().doFileTimestamp(fileName, dataTime);

			this.log.debug(this.name + "：localFile,当前要解析的文件为:" + fileName);
			this.parser.setFileName(fileName);
			try
			{
				this.parser.parseData();
			}
			catch (Exception e)
			{
				this.errorlog.error(this.name + ": 文件解析失败(" + fileName + "),原因:", e);
			}
		}

		return true;
    }

	public void configure()
		throws Exception
    {
    }

	public boolean doAfterAccess()
    	throws Exception
    {
		boolean flag = false;

		String cmd = this.taskInfo.getShellCmdFinish();
		if (Util.isNotNull(cmd))
		{
			flag = new ExternalCmd().execute(cmd) == 0;
		}
		return flag;
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