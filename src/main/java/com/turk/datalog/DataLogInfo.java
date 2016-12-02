package com.turk.datalog;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.Date;

public class DataLogInfo
  	implements Cloneable
{
	private Timestamp logTime;
	private int taskId;
	private String taskDescription;
	private String taskType;
	private String taskStatus;
	private String taskDetail;
	private String taskException;
	private Timestamp dataTime;
	private long costTime;
	private String taskResult;
	public static final String TYPE_NORMAL = "正常任务";
	public static final String TYPE_RTASK = "补采任务";
	public static final String STATUS_START = "开始";
	public static final String STATUS_PARSE = "解析";
	public static final String STATUS_DIST = "入库";
  	public static final String STATUS_END = "结束";
  	public static final String RESULT_OK = "成功";
  	public static final String RESULT_POK = "部分成功";
  	public static final String RESULT_FAIL = "失败";

  	public DataLogInfo()
  	{
  	}

  	public DataLogInfo(int taskId, String taskDescription, String taskType, String taskStatus, String taskDetail, String taskException, Timestamp dataTime, long costTime, String taskResult)
  	{
  		this(null, taskId, taskDescription, taskType, taskStatus, taskDetail, taskException, dataTime, costTime, taskResult);
  	}

  	public DataLogInfo(Timestamp logTime, int taskId, String taskDescription, String taskType, String taskStatus, String taskDetail, String taskException, Timestamp dataTime, long costTime, String taskResult)
  	{
  		this.logTime = logTime;
  		this.taskId = taskId;
  		this.taskDescription = taskDescription;
  		this.taskType = taskType;
  		this.taskStatus = taskStatus;
  		this.taskDetail = taskDetail;
  		this.taskException = taskException;
  		this.dataTime = dataTime;
  		this.costTime = costTime;
  		this.taskResult = taskResult;
  	}

  	public Timestamp getLogTime()
  	{
  		return this.logTime;
  	}

  	public void setLogTime(Timestamp logTime)
  	{
  		this.logTime = logTime;
  	}

  	public void setLogTime(Date logTime)
  	{
  		this.logTime = (logTime != null ? new Timestamp(logTime.getTime()) : null);
  	}

  	public int getTaskId()
  	{
  		return this.taskId;
  	}

  	public void setTaskId(int taskId)
  	{
  		this.taskId = taskId;
  	}

  	public String getTaskDescription()
  	{
  		return this.taskDescription == null ? "" : this.taskDescription;
  	}

  	public void setTaskDescription(String taskDescription)
  	{
  		this.taskDescription = taskDescription;
  	}

  	public String getTaskType()
  	{
  		return this.taskType == null ? "" : this.taskType;
  	}
  	
  	public void setTaskType(String taskType)
  	{
  		this.taskType = taskType;
  	}

  	public String getTaskStatus()
  	{
  		return this.taskStatus == null ? "" : this.taskStatus;
  	}

  	public void setTaskStatus(String taskStatus)
  	{
  		this.taskStatus = taskStatus;
  	}

  	public String getTaskDetail()
  	{
  		return this.taskDetail == null ? "" : this.taskDetail;
  	}

  	public void setTaskDetail(String taskDetail)
  	{
  		this.taskDetail = taskDetail;
  	}

  	public String getTaskException()
  	{
  		return this.taskException == null ? "" : this.taskException;
  	}

  	public void setTaskException(String taskException)
  	{
  		this.taskException = taskException;
  	}

  	public void setTaskException(Throwable taskException)
  	{
  		if (taskException == null)
  		{
  			this.taskException = "";
  		}
  		else
  		{
  			StringWriter writer = new StringWriter();
  			taskException.printStackTrace(new PrintWriter(writer));
  			writer.flush();
  			try
  			{
  				writer.close();
  			}
  			catch (IOException localIOException)
  			{
  			}
  			this.taskException = writer.toString();
  		}
  	}

  	public Timestamp getDataTime()
  	{
  		return this.dataTime;
  	}

  	public void setDataTime(Timestamp dataTime)
  	{
  		this.dataTime = dataTime;
  	}
  	
  	public void setDataTime(Date dataTime)
  	{
  		this.dataTime = (dataTime != null ? new Timestamp(dataTime.getTime()) : null);
  	}

  	public long getCostTime()
  	{
  		return this.costTime;
  	}

  	public void setCostTime(long costTime)
  	{
  		this.costTime = costTime;
  	}

  	public String getTaskResult()
  	{
  		return this.taskResult == null ? "" : this.taskResult;
  	}

  	public void setTaskResult(String taskResult)
  	{
  		this.taskResult = taskResult;
  	}

  	public void addLog()
  	{
  		try
  		{
  			DataLogMgr.getInstance().addLog((DataLogInfo)clone());
  		}
  		catch (CloneNotSupportedException e)
  		{
  			e.printStackTrace();
  		}
  	}
}