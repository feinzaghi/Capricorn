package com.turk.delayprobe;

import com.turk.Config.SystemConfig;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.turk.task.CollectObjInfo;
import com.turk.task.RegatherObjInfo;
import com.turk.util.LogMgr;
import com.turk.util.Util;

/**
 * ̽�����
 * @author Administrator
 *
 */
public class DelayProbeMgr
{
	private static Map<String, Boolean> firsts = new HashMap<String, Boolean>();

	private static Set<String> errors = new HashSet<String>();

	private static Map<Integer, TaskDataEntry> taskEntrys = new HashMap<Integer, TaskDataEntry>();

	public static int time = -1;

	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	/**
	 * ��ȡ̽������
	 * @param tempTasks
	 * @return
	 */
	public static List<CollectObjInfo> probe(List<CollectObjInfo> tempTasks)
	{
		if (!SystemConfig.getInstance().isEnableDelayProbe()) 
			return null;
		final List<CollectObjInfo> results = new ArrayList<CollectObjInfo>();
		List<Thread> threads = new ArrayList<Thread>();
		for (final CollectObjInfo c : tempTasks)
		{
			if (!validate(c))
			{//����Ƿ�����̽��Ҫ��
				continue;
			}

			final String key = c.getTaskID() + "[" + 
				Util.getDateString(c.getLastCollectTime()) + "]";
			threads.add(new Thread("  ProbeThread - " + key + "  ")
			{
				public void run()
				{
					TaskDataEntry newTde = new TaskDataEntry(c);
					int probeTime = c.getProbeTime();
					if (!newTde.isNoError())
					{
						String log = key + ":ִ��̽��ʱ�쳣����ʱ��㲻��ʹ��̽��";
						DelayProbeMgr.logger.error(log);
						newTde.setProbeLogger(new ProbeLogger(c.getTaskID()));
						newTde.getProbeLogger().println(log);
						newTde.getProbeLogger().dispose();
						DelayProbeMgr.errors.add(key);
						return;
					}
					if (DelayProbeMgr.taskEntrys.containsKey(Integer.valueOf(c.getTaskID())))
					{
						TaskDataEntry tde = (TaskDataEntry)DelayProbeMgr.taskEntrys.get(Integer.valueOf(c.getTaskID()));
						newTde.setProbeCount(tde.getProbeCount() + 1);
						newTde.setPre(tde);
						newTde.setEqCount(tde.getEqCount());
						newTde.setProbeLogger(tde.getProbeLogger());
					}
					else
					{
						newTde.setProbeLogger(new ProbeLogger(c.getTaskID()));
						newTde.setProbeCount(1);
					}
					boolean bEq = newTde.compare();
					String result = bEq ? "���" : "�����";
					String log = "�����: " + 
						c.getTaskID() + 
						", ʱ���: " + 
						Util.getDateString(c.getLastCollectTime()) + 
						", �������õ���ʱ(����):" + 
						c.getCollectTimePos() + 
						", ̽����(����):" + 
						SystemConfig.getInstance().getProbeInterval() + 
						", ̽�⿪ʼʱ�䣺" + 
						probeTime + 
						"��" + 
						", ̽�����: " + 
						newTde.getProbeCount() + (
								newTde.getPre() == null ? "(�״�̽�⣬�����ϴ����ݣ����ܱȽ�)" : new StringBuilder(", �ȽϽ��: ")
								.append(result).toString());
					newTde.getProbeLogger().println(log);
					DelayProbeMgr.logger.debug(log);
					newTde.getProbeLogger().println("");
					DelayProbeMgr.taskEntrys.put(Integer.valueOf(c.getTaskID()), newTde);
					boolean b = (newTde.isNoError()) && 
						(newTde.getEqCount() >= SystemConfig.getInstance().getDelayProbeTimes());
					if (b)
					{
						log = key + "����" + 
							SystemConfig.getInstance().getDelayProbeTimes() + 
							"�αȽϣ�������δ�仯��ȷ�Ͽ��Կ�ʼ�ɼ�";
						newTde.getProbeLogger().println(log);
						DelayProbeMgr.logger.debug(log);
						newTde.getProbeLogger().println("");
						newTde.getProbeLogger().dispose();
						DelayProbeMgr.taskEntrys.remove(Integer.valueOf(c.getTaskID()));
						results.add(c);
					}
				} } );
		}
		for (Thread t : threads)
		{
			t.start();
		}

		for (Thread t : threads)
		{
			try
			{
				t.join();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}

		return results;
	}

	public static Map<Integer, TaskDataEntry> getTaskEntrys()
	{
		return taskEntrys;
	}

	/**
	 * ��֤�������Ƿ��������̽��Ҫ��
	 * @param c
	 * @return
	 */
	private static boolean validate(CollectObjInfo c)
	{
		//���ڲ��������Լ���Сʱ�ɼ��������������̽��
		if ((c == null) || ((c instanceof RegatherObjInfo)) || 
				(c.getPeriod() != 3)) 
			return false;

		Calendar calendar = Calendar.getInstance();
		long currTime = calendar.getTimeInMillis();
		int currMinute = calendar.get(12);
		boolean isRightTime = 
			(currTime >= c.getLastCollectTime().getTime() + 
					c.getPeriodTime()) && 
					((currMinute >= c.getProbeTime()) || 
    		  (currTime >= c.getLastCollectTime().getTime() + 
    				  c.getPeriodTime() * 2L)) && 
    				  (currTime < c.getLastCollectTime().getTime() + 
    						  c.getCollectTimePos() * 60 * 1000);
		
		if (!isRightTime) 
			return false;

		int probeTime = c.getProbeTime();
		if (probeTime < 0) 
			return false;
		
		int type = c.getCollectType();
		
		if ((type != 6) && (type != 60) && (type != 3) && (type != 9)) 
			return false;
		if (((type == 3) || (type == 9)) && 
				(!SystemConfig.getInstance().isProbeFTP())) 
			return false;
		
		String key = c.getTaskID() + "[" + 
		Util.getDateString(c.getLastCollectTime()) + "]";
		boolean isFirstProbe = firsts.containsKey(key) ? ((Boolean)firsts.get(key)).booleanValue() : true;
		if ((time % SystemConfig.getInstance().getProbeInterval() != 0) && 
				(!isFirstProbe)) 
			return false;
		firsts.put(key, Boolean.valueOf(false));
		return !errors.contains(key);
	}
}