package com.turk.clusters.common;

import java.io.FileWriter;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.turk.clusters.master.TaskManage;
import com.turk.clusters.model.TaskMsg;
import com.turk.config.ConstDef;
import com.turk.socket.Client;

import net.sf.json.JSONObject;

import com.turk.util.Util;

/**
 * 集群补采
 * @author Administrator
 *
 */
public class ReTaskObjInfo extends TaskObjInfo{
	private static final long serialVersionUID = 1L;
	private String filePath = "";
	private int reAdoptType = 1;
	private int collectTimes = 0;

	public String strFileName = "";
	public int recondCount = 0;
	public FileWriter m_hFile = null;
	public int tableIndex = 0;

	private List<Integer> tableIndexes = new ArrayList<Integer>();
  	private Timestamp stamptime;

  	public ReTaskObjInfo(int ID, int taskID)
  	{
  		super(taskID,"192.168.0.20",9528);
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

  	public void buildObj(ResultSet rs) throws Exception
  	{
  		try
  		{
	  		super.buildObj(rs);
	  		
	  		
	  		objMap.put("id", String.valueOf(rs.getInt("ID")));
	  		objMap.put("readopttype", String.valueOf(rs.getInt("READOPTTYPE")));
	  		
	  		if(objMap.containsKey("collecttimes"))
	  		{
	  			objMap.remove("collecttimes");
	  		}
	  		objMap.put("collecttimes", String.valueOf(rs.getInt("COLLECTDEGRESS")));
	  		
	  		
	  		//this.keyID = rs.getInt("ID");
	  		//this.reAdoptType = rs.getInt("READOPTTYPE");
	    	//this.collectTimes = rs.getInt("COLLECTDEGRESS");
	
	  		if(objMap.containsKey("stamptime"))
	  		{
	  			objMap.remove("stamptime");
	  		}
	    	Timestamp ts = rs.getTimestamp("STAMPTIME");
	    	if (ts == null)
	    	{
	    		objMap.put("stamptime", Util.getDateString_Standard_ss(new Timestamp(new Date().getTime())));
	    		//this.stamptime = new Timestamp(new Date().getTime());
	    	}
	    	else
	    	{
	    		objMap.put("stamptime", Util.getDateString_Standard_ss(ts));
		    }
	
	    	
	    	if(objMap.containsKey("filepath"))
	  		{
	  			objMap.remove("filepath");
	  		}
	    	this.filePath = "";
	    	if (rs.getClob("FILEPATH") != null)
	    	{
	    		//this.filePath = ConstDef.ClobParse(rs.getClob("FILEPATH"));
	    		objMap.put("filepath", ConstDef.ClobParse(rs.getClob("FILEPATH")));
	    	}
	
	    	super.setLastCollectTime(rs.getTimestamp("COLLECTTIME"));
	
	    	if(objMap.containsKey("collecttime"))
	  		{
	  			objMap.remove("collecttime");
	  		}
	    	objMap.put("collecttime", Util.getDateString_Standard_ss(rs.getTimestamp("COLLECTTIME")));
	    	
	    	if ((this.filePath != null) && (this.filePath.length() != 0))
	    	{
	    		if(objMap.containsKey("collectpath"))
	      		{
	      			objMap.remove("collectpath");
	      		}
	    		objMap.put("collectpath", ConstDef.ClobParse(rs.getClob("FILEPATH")));
	    		//this.collectPath = this.filePath;
	    	}
  		}
  		catch(Exception ex)
  		{
  			log.error("ReTaskObjInfo-buildObj(ResultSet rs)",ex);
  		}
  	}

  	/**
  	 * 构建采集任务对象
  	 */
  	public void buildObj(ResultSet rs, Date scantime)
  	{
  		//TaskMgr tmgr = TaskMgr.getInstance();

  		//if (tmgr.isActive(rs.getInt("ID"), rs.getInt("TASKID"), ConstDef.ClobParse(rs.getClob("FILEPATH")), rs.getTimestamp("COLLECTTIME"), true))
  		//{
  		//	this.log.debug(this.sysName + " is active");
  		//	return;
  		//}

  		try
  		{
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
	  		{
	  			//int id = this.keyID - 10000000;
	  			//tmgr.updateRegatherState(-1, id);
	  			return;
	  		}
	
	  		addReTask(scantime);
  		}
  		catch(Exception ex)
  		{
  			log.error("Build re-c task error",ex);
  		}
  	}


  	/**
  	 * 添加采集对象
  	 */
  	private void addReTask(Date scantime)
  	{
  		//判断任务是否存在，将任务加入列表中
  		if (TaskManage.getInstance().addTask(this))
  		{
  			TaskMsg task = new TaskMsg();
  			task.setMsgID(2001);
  			task.setTaskID(this.getTaskID());
  			task.setIsReCLT(1);
  			task.setObjMap(this.objMap);
  			
	  		JSONObject jsonObject = JSONObject.fromObject(task);
			log.debug("2001-MSG：" + jsonObject.toString());
		    Client clt = new Client(this.server,
		    		this.port);
		    String Result = clt.SendMsg(jsonObject.toString());
		    if(Result.equals("Done"))
		    {
		    	log.debug("2001-MSG:补采任务["+this.keyID+"]发送成功");
		    }
  		}
  	}
  	
  	public boolean doAfterCollect()
  	{
  		int id = this.keyID - 10000000;

  		boolean b = TaskManage.getInstance().updateRegatherState(3, id);

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
