package com.turk.task;

import com.turk.Config.ConstDef;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.regex.Pattern;
import javax.servlet.jsp.jstl.sql.Result;
import org.apache.log4j.Logger;
import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public final class IgnoresMgr
{
	private static IgnoresMgr instance;
	private static final Logger logger = LogMgr.getInstance().getSystemLogger();

	private Map<Integer, List<IgnoresInfo>> ignoreses = new HashMap();

	public static synchronized IgnoresMgr getInstance()
	{
		if (instance != null)
			return instance;
		instance = new IgnoresMgr();
		return instance;
	}

	public synchronized List<IgnoresInfo> getIgnoresesByTaskId(int taskId)
	{
		return this.ignoreses.containsKey(Integer.valueOf(taskId)) ? (List)this.ignoreses.get(Integer.valueOf(taskId)) : new ArrayList();
	}

	public synchronized IgnoresInfo checkIgnore(int taskId, String path, Timestamp time)
  	{
		String aPath = path.replace('\\', '/');
		List<IgnoresInfo> list = getIgnoresesByTaskId(taskId);
		IgnoresInfo ignoresInfo = null;
		for (IgnoresInfo ig : list)
		{
			if ((!logicEquals(aPath.toLowerCase(), ConstDef.ParseFilePath(ig.getPath(), time).toLowerCase())) || 
					(!ig.isUsed()))
				continue;
			ignoresInfo = ig;
			break;
		}

		return ignoresInfo;
  	}

	private IgnoresMgr()
	{
		load();
	}

	private void load()
	{
		String sql = "select * from utl_conf_ignores where isused=1";
		Result result = null;
		try
		{
			result = CommonDB.queryForResult(sql);
			if (result != null)
			{
				SortedMap[] maps = result.getRows();
				int size = maps.length;
				for (int i = 0; i < size; i++)
				{
					SortedMap m = maps[i];
					String path = m.get("path").toString();
					if (Util.isNull(path))
					{
						continue;
					}
					int taskId = Integer.parseInt(m.get("taskid").toString());
					int intIsused = Integer.parseInt(m.get("isused").toString());
					if (this.ignoreses.containsKey(Integer.valueOf(taskId)))
					{
						((List)this.ignoreses.get(Integer.valueOf(taskId))).add(new IgnoresInfoImpl(path, taskId, intIsused == 1, intIsused));
					}
					else
					{
						List list = new ArrayList();
						list.add(new IgnoresInfoImpl(path, taskId, intIsused == 1, intIsused));
						this.ignoreses.put(Integer.valueOf(taskId), list);
					}
				}
			}
		}
		catch (Exception localException)
		{
		}
	}

	private boolean logicEquals(String shortFileName, String fileName)
	{
		if ((!fileName.contains("*")) && (!fileName.contains("?"))) return shortFileName.contains(fileName);
		
		String s1 = shortFileName.replaceAll("\\.", "");
    	String s2 = fileName.replaceAll("\\.", "");
    	s1 = s1.replaceAll("\\+", "");
    	s2 = s2.replaceAll("\\+", "");
    	s2 = s2.replaceAll("\\*", ".*");
    	s2 = s2.replaceAll("\\?", ".");
    	s2 = ".*" + s2 + ".*";
    	return Pattern.matches(s2, s1);
	}

	public static void main(String[] args)
		throws ParseException
  	{
		IgnoresMgr mgr = getInstance();
		IgnoresInfo info = mgr.checkIgnore(1027, "aac:\\usr\\cfg_perf_luaac_2010", new Timestamp(Util.getDate1("2010-10-28 00:00:00").getTime()));
		System.out.println(info);
  	}

	private class IgnoresInfoImpl
		implements IgnoresInfo
	{
		String path = "";
		int taskId;
		boolean isUsed;
    	int intIsUsed;

    	public IgnoresInfoImpl(String path, int taskId, boolean isUsed, int intIsUsed)
    	{
    		this.path = path;
    		this.taskId = taskId;
    		this.isUsed = isUsed;
    		this.intIsUsed = intIsUsed;
    	}

    	public String getPath()
    	{
    		return this.path;
    	}

    	public int getTaskId()
    	{
    		return this.taskId;
    	}

    	public boolean isUsed()
    	{
    		return this.isUsed;
    	}

    	public void setNotUsed()
    	{
    		String sql = "update utl_conf_ignores set isused=0,modif_time=sysdate where taskid=" + 
    		this.taskId + " and path='" + this.path + "'";
    		try
    		{
    			CommonDB.executeUpdate(sql);
    			this.isUsed = false;
    		}
    		catch (SQLException localSQLException)
    		{
    		}
    	}
    	
    	public boolean equals(Object obj)
    	{
    		if (obj != null)
    		{
    			if (obj == this) 
    				return true;
    			if ((obj instanceof IgnoresInfoImpl))
    			{
    				IgnoresInfoImpl instance = (IgnoresInfoImpl)obj;

    				return (instance.getTaskId() == getTaskId()) && 
    					(instance.getPath().equals(getPath()));
    			}
    		}
    		return false;
    	}

    	public String toString()
    	{
    		return "{tastid:" + this.taskId + ", path:" + this.path + ", isused:" + 
    			this.intIsUsed + "}";
    	}
	}
}