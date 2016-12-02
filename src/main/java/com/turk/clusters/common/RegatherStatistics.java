package com.turk.clusters.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.task.RegatherStatisticsObj;
import com.turk.util.LogMgr;
import com.turk.util.Util;
import com.turk.access.AbstractDBAccessor;

public class RegatherStatistics
{
	private Map<Long, RegatherStatisticsObj> statisticsMap;
	public int MAX_REGATHER_TIMES = 10;
	private static Logger log = LogMgr.getInstance().getSystemLogger();

	private static RegatherStatistics instance = null;

	private RegatherStatistics()
	{
		this.statisticsMap = new HashMap<Long, RegatherStatisticsObj>();
	}

	public static synchronized RegatherStatistics getInstance()
	{
		if (instance == null)
		{
			instance = new RegatherStatistics();
		}
		return instance;
	}

	public synchronized int getRecltTimes(ReTaskObjInfo obj)
	{
		String str = String.valueOf(obj.getTaskID()) + 
			Util.getDateString(obj.getLastCollectTime()) + 
			obj.getCollectPath();
		long key = Util.crc32(str);
		if (this.statisticsMap.containsKey(Long.valueOf(key)))
		{
    	  return ((RegatherStatisticsObj)this.statisticsMap.get(Long.valueOf(key))).getTimes() + 1;
		}

		return 1;
	}

	public synchronized boolean check(ReTaskObjInfo obj)
	{
		boolean b = true;

		this.MAX_REGATHER_TIMES = obj.getMaxReCollectTime();

		if ((obj.getCollectThread() instanceof AbstractDBAccessor))
		{
			if (obj.getMaxReCollectTime() <= -2)
			{
				this.MAX_REGATHER_TIMES = Math.abs(obj.getMaxReCollectTime() + 2);
			}
		}

		int taskID = obj.getTaskID();
		String filePath = obj.getCollectPath();
		String strCollectTime = Util.getDateString(obj.getLastCollectTime());
		String str = String.valueOf(taskID) + strCollectTime + filePath;
	
		long key = Util.crc32(str);
		if (this.statisticsMap.containsKey(Long.valueOf(key)))
		{
			RegatherStatisticsObj sObj = (RegatherStatisticsObj)this.statisticsMap.get(Long.valueOf(key));
			if (sObj == null) {
				this.statisticsMap.remove(Long.valueOf(key));
			}
			int currentTimes = sObj.getTimes();
			if (currentTimes >= this.MAX_REGATHER_TIMES)
			{
				b = false;
				this.statisticsMap.remove(Long.valueOf(key));
				log.info("补采任务" + obj + ": 已达到最大补采次数.(" + filePath + " " + 
						strCollectTime + " 此时补采的ID为:" + (
								obj.getKeyID() - 10000000) + ")");
			}
			else
			{
				sObj.setTimes(currentTimes + 1);
				log.info("补采任务" + obj + ": 当前补采次数.(" + sObj.getTimes() + " " + 
						filePath + " " + strCollectTime + " 此时补采的ID为:" + (
								obj.getKeyID() - 10000000) + ")");
			}
		}
		else {
			this.statisticsMap.put(Long.valueOf(key), new RegatherStatisticsObj(key, 1));
		}
		return b;
	}
}
