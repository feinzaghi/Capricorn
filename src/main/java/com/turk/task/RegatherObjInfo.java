package com.turk.task;

import com.turk.access.AbstractAccessor;
import com.turk.config.ConstDef;
import com.turk.framework.Factory;

import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import com.turk.util.ThreadPool;
import com.turk.util.Util;

/**
 * 补采对象
 * @author Administrator
 *
 */
public class RegatherObjInfo extends CollectObjInfo
{
	private static final long serialVersionUID = 1L;
	private String filePath = "";
	private int reAdoptType = 1;
	private int collectTimes = 0;

	public String strFileName = "";
	public int recondCount = 0;
	public FileWriter m_hFile = null;
	public int tableIndex = 0;

	private List<Integer> tableIndexes = new ArrayList();
  	private Timestamp stamptime;

  	public RegatherObjInfo(int ID, int taskID)
  	{
  		super(taskID);
  		this.keyID = ID;
  		this.sysName = (taskID + "-" + (ID - 10000000));
  	}

  	public int getReAdoptType()
  	{
  		return this.reAdoptType;
  	}
  	
  	public void setReAdoptType(int type)
  	{
  		this.reAdoptType = type;
  	}

  	public int getCollectTimes()
  	{
  		return this.collectTimes;
  	}

  	public void setCollectTimes(int times)
  	{
  		this.collectTimes = times;
  	}

  	public String getFilePath()
  	{
  		return this.filePath;
  	}

  	public void setFilePath(String path)
  	{
  		this.filePath = path;
  	}
  	
  	/**
  	 * 构建采集任务
  	 * @param rs
  	 * @param scantime
  	 * @throws Exception
  	 */
  	public void buildObj(HashMap<String,String> rs, Date scantime) throws Exception
  	{
  		TaskMgr tmgr = TaskMgr.getInstance();

  		if (tmgr.isActive(Integer.parseInt(rs.get("id")), 
  				Integer.parseInt(rs.get("taskid")), 
  				rs.get("filepath"), new Timestamp(Util.getDate(rs.get("collecttime"), "yyyy-MM-dd HH:mm:ss").getTime()), true))
  		{
  			this.log.debug(this.sysName + " is active");
  			return;
  		}
  		
  		buildObj(rs);
  		
  		this.keyID = Integer.parseInt(rs.get("id"));
  		this.reAdoptType = Integer.parseInt(rs.get("readopttype"));
    	this.collectTimes = Integer.parseInt(rs.get("collecttimes"));

    	Timestamp ts = new Timestamp(Util.getDate(rs.get("stamptime"), "yyyy-MM-dd HH:mm:ss").getTime());
    	if (ts == null)
    	{
    		this.stamptime = new Timestamp(new Date().getTime());
    	}
    	else
    	{
    		this.stamptime = ts;
	    }

    	this.filePath = "";
    	if (rs.get("filepath") != null)
    	{
    		this.filePath = rs.get("filepath");
    	}

    	super.setLastCollectTime(new Timestamp(Util.getDate(rs.get("collecttime"), "yyyy-MM-dd HH:mm:ss").getTime()));

    	if ((this.filePath != null) && (this.filePath.length() != 0))
    	{
    		this.collectPath = this.filePath;
    	}

    	int recltTimes = RegatherStatistics.getInstance().getRecltTimes(this);

  		long time = this.stamptime.getTime() + getRedoTimeOffset() * recltTimes * 60 * 1000;
  		this.log.debug(this.sysName + " 第" + recltTimes + "次补采,设置的补采时延:" + 
  				getRedoTimeOffset() + "分钟,实际时延:" + 
  				getRedoTimeOffset() * recltTimes + 
  				"分钟(设置的补采时延 * 补采次数),预计补采开始时间:" + 
  				Util.getDateString(new Timestamp(time)));
  		if (time > scantime.getTime()) return;

  		boolean b = RegatherStatistics.getInstance().check(this);
  		if (!b)
  		{//如不需要继续补采，择修改补采状态
  			int id = this.keyID - 10000000;
  			tmgr.updateRegatherState(-1, id);
  			return;
  		}

  		addTaskItem(scantime);
  	}
  	

  	public void buildObj(ResultSet rs) throws Exception
  	{
  		super.buildObj(rs);

  		this.keyID = rs.getInt("ID");
  		this.reAdoptType = rs.getInt("READOPTTYPE");
    	this.collectTimes = rs.getInt("COLLECTDEGRESS");

    	Timestamp ts = rs.getTimestamp("STAMPTIME");
    	if (ts == null)
    	{
    		this.stamptime = new Timestamp(new Date().getTime());
    	}
    	else
    	{
    		this.stamptime = ts;
	    }

    	this.filePath = "";
    	if (rs.getClob("FILEPATH") != null)
    	{
    		this.filePath = ConstDef.ClobParse(rs.getClob("FILEPATH"));
    	}

    	super.setLastCollectTime(rs.getTimestamp("COLLECTTIME"));

    	if ((this.filePath != null) && (this.filePath.length() != 0))
    	{
    		this.collectPath = this.filePath;
    	}
  	}

  	/**
  	 * 构建采集任务对象
  	 */
  	public void buildObj(ResultSet rs, Date scantime)
  		throws Exception
  	{
  		TaskMgr tmgr = TaskMgr.getInstance();

  		if (tmgr.isActive(rs.getInt("ID"), rs.getInt("TASKID"), ConstDef.ClobParse(rs.getClob("FILEPATH")), rs.getTimestamp("COLLECTTIME"), true))
  		{
  			this.log.debug(this.sysName + " is active");
  			return;
  		}

  		buildObj(rs);

  		int recltTimes = RegatherStatistics.getInstance().getRecltTimes(this);

  		long time = this.stamptime.getTime() + getRedoTimeOffset() * recltTimes * 60 * 1000;
  		this.log.debug(this.sysName + " 第" + recltTimes + "次补采,设置的补采时延:" + 
  				getRedoTimeOffset() + "分钟,实际时延:" + 
  				getRedoTimeOffset() * recltTimes + 
  				"分钟(设置的补采时延 * 补采次数),预计补采开始时间:" + 
  				Util.getDateString(new Timestamp(time)));
  		if (time > scantime.getTime()) return;

  		boolean b = RegatherStatistics.getInstance().check(this);
  		if (!b)
  		{//如不需要继续补采，择修改补采状态
  			int id = this.keyID - 10000000;
  			tmgr.updateRegatherState(-1, id);
  			return;
  		}

  		addTaskItem(scantime);
  	}

  	/**
  	 * 添加采集对象
  	 */
  	protected void addTaskItem(Date scantime)
  	{
  		if (TaskMgr.getInstance().addTask(this))
  		{
  			AbstractAccessor accessor = Factory.createAccessor(this);
  			setCollectThread(accessor);
  			//accessor.start();
  			accessor.setBeginExceuteTime(new Date());
  			ThreadPool.getInstance().addTask(accessor);
  			this.log.debug("补采任务：" + this.sysName + " 已被加入采集队列中.");
  		}
  	}

  	public boolean doAfterCollect()
  	{
  		int id = this.keyID - 10000000;

  		boolean b = TaskMgr.getInstance().updateRegatherState(3, id);

  		return b;
  	}

  	public Timestamp getStamptime()
  	{
  		return this.stamptime;
  	}

  	public void setStamptime(Timestamp stamptime)
  	{
  		this.stamptime = stamptime;
  	}

  	public String toString()
  	{
  		return this.sysName;
  	}

  	public void addTableIndex(int index)
  	{
  		if (!this.tableIndexes.contains(Integer.valueOf(index)))
  		{
  			this.tableIndexes.add(Integer.valueOf(index));
  		}
 	 }

  	public boolean existsInTableIndexes(int index)
  	{
  		return this.tableIndexes.contains(Integer.valueOf(index));
  	}

  	public boolean isEmptyTableIndexes()
  	{
  		return this.tableIndexes.isEmpty();
  	}
}