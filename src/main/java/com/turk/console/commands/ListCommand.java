package com.turk.console.commands;

import java.sql.Timestamp;
import java.util.List;

import com.turk.console.common.console.io.CommandIO;
import com.turk.task.CollectObjInfo;
import com.turk.task.RegatherObjInfo;
import com.turk.task.TaskMgr;

/**
 * 列表查看当前运行的采集任务
 * @author Administrator
 *
 */
public class ListCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		List<CollectObjInfo> lst = TaskMgr.getInstance().list();
		if (lst.size() == 0)
		{
			io.println("暂无运行任务");
			return true;
		}

		long now = System.currentTimeMillis();
		for (CollectObjInfo obj : lst)
		{
			int taskID = obj.getTaskID();
			String des = obj.getDescribe();
			Timestamp dataTime = obj.getLastCollectTime();

			String flag = "";
			if ((obj instanceof RegatherObjInfo))
				flag = "-" + String.valueOf(obj.getKeyID() - 10000000);
			String cost = "";
			
			long fast = now - obj.startTime.getTime();

			if (fast < 60000L) {
				cost = Math.round((float)(fast / 1000L)) + " 秒";
			}
			else
			{
				cost = Math.round((float)(fast / 60000L)) + " 分钟";
			}
			io.println(taskID + flag + "   " + dataTime + "   " + des + "  " + cost);
		}
		io.println("总计： " + lst.size() + " 个");
		return true;
    }
}