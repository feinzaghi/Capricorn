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
			io.println("kill�﷨����,ȱ��������. ����help/?��ȡ�������");
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
			io.println("kill�﷨����,��������������. ����help/?��ȡ�������");
			return true;
		}

		Task obj = ThreadPool.getInstance().getTask(taskID);
		
		//CollectObjInfo obj = TaskMgr.getInstance().getObjByID(taskID);
		if (obj == null)
		{
			//obj = TaskMgr.getInstance().getObjByID(taskID + 10000000);
			if (obj == null)
			{
				io.println("ָ���������ŵ�ǰ��������״̬���������Ų�����");
				return true;
			}

		}

		//String des = obj.getDescribe();
		//Timestamp dataTime = obj.getLastCollectTime();
		String strLine = io.readLine("�Ƿ�Ҫɱ������(" + taskID + ", " + obj.toString() + " )?   [y|n]  ");

		if ((strLine.equalsIgnoreCase("n")) || (strLine.equalsIgnoreCase("no"))) {
			return true;
		}
		if ((!strLine.equalsIgnoreCase("n")) && (!strLine.equalsIgnoreCase("no")) && 
				(!strLine.equalsIgnoreCase("y")) && 
				(!strLine.equalsIgnoreCase("yes")))
		{
			io.println("�Ƿ�����,��������.");
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