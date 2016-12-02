package com.turk.access;

import com.turk.collect.SocketCollect;
import com.turk.util.Parsecmd;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * Socket 访问
 * @author Administrator
 *
 */
public class SocketAccessor extends AbstractAccessor
{
	public boolean access()
		throws Exception
	{
		this.log.debug(this.name + ": 开始通过Socket连接方式处理字符流.");
		SocketCollect socket = new SocketCollect(this.taskInfo);
		socket.start();
		return true;
	}

	public void configure()
		throws Exception
    {
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