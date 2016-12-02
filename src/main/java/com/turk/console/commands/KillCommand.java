package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

import com.turk.util.Task;
import com.turk.util.ThreadPool;

public class KillCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		if ((args == null) || (args.length < 1))
		{
			io.println("kill语法错误,缺少任务编号. 输入help/?获取命令帮助");
			return true;
		}

		String strTaskID = args[0];
		int taskID = -1;
		try
		{
			taskID = Integer.parseInt(strTaskID);
		}
		catch (NumberFormatException localNumberFormatException)
		{
		}
		if (taskID == -1)
		{
			io.println("kill语法错误,任务编号输入有误. 输入help/?获取命令帮助");
			return true;
		}

		Task obj = ThreadPool.getInstance().getTask(taskID);
		
		//CollectObjInfo obj = TaskMgr.getInstance().getObjByID(taskID);
		if (obj == null)
		{
			//obj = TaskMgr.getInstance().getObjByID(taskID + 10000000);
			if (obj == null)
			{
				io.println("指定的任务编号当前不在运行状态或者任务编号不存在");
				return true;
			}

		}

		//String des = obj.getDescribe();
		//Timestamp dataTime = obj.getLastCollectTime();
		String strLine = io.readLine("是否要杀死任务(" + taskID + ", " + obj.toString() + " )?   [y|n]  ");

		if ((strLine.equalsIgnoreCase("n")) || (strLine.equalsIgnoreCase("no"))) {
			return true;
		}
		if ((!strLine.equalsIgnoreCase("n")) && (!strLine.equalsIgnoreCase("no")) && 
				(!strLine.equalsIgnoreCase("y")) && 
				(!strLine.equalsIgnoreCase("yes")))
		{
			io.println("非法输入,放弃操作.");
			return true;
		}

		//Task thrd = obj.getCollectThread();
		if (obj != null)
		{
			//thrd.interrupt();
			obj.stopTask();
			//TaskMgr.getInstance().delActiveTask(taskID, obj instanceof RegatherObjInfo);
		}
		return true;
    }
}