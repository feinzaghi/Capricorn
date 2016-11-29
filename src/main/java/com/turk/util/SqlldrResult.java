package com.turk.util;

import java.util.List;

public class SqlldrResult extends Result
{
	private String maxFileSize;
	private boolean isOracleLog;
	private String tableName;
	private int loadSuccCount = 0;
	private int data;
	private int when;
  	private int nullField;
  	private int skip;
  	private int read;
 	private int refuse;
 	private int abandon;
 	private String startTime;
 	private String endTime;
 	private double runTime;
 	private List<String> ruleList;

 	public SqlldrResult()
 	{
 		this(0);
 	}

 	public SqlldrResult(int code, String message, Result cause)
 	{
 		super(code, message, cause);
 	}

 	public SqlldrResult(int code, String message)
 	{
 		super(code, message);
 	}

 	public SqlldrResult(int code)
 	{
 		super(code);
 	}

 	public SqlldrResult(Result cause)
 	{
 		super(cause);
 	}

 	public String getMaxFileSize()
 	{
 		return this.maxFileSize;
 	}

 	public void setMaxFileSize(String maxFileSize)
 	{
 		this.maxFileSize = maxFileSize;
 	}

 	public boolean isOracleLog()
 	{
 		return this.isOracleLog;
 	}

 	public void setOracleLog(boolean isOracleLog)
 	{
 		this.isOracleLog = isOracleLog;
 	}

 	public String getTableName()
 	{
 		return this.tableName;
 	}

 	public void setTableName(String tableName)
 	{
 		this.tableName = tableName;
 	}

 	public int getLoadSuccCount()
 	{
 		return this.loadSuccCount;
 	}

 	public void setLoadSuccCount(int loadSuccCount)
 	{
 		this.loadSuccCount = loadSuccCount;
 	}

 	public int getData()
 	{
 		return this.data;
 	}

 	public void setData(int data)
 	{
 		this.data = data;
  	}

 	public int getWhen()
 	{
 		return this.when;
 	}

 	public void setWhen(int when)
 	{
 		this.when = when;
 	}

 	public int getNullField()
 	{
 		return this.nullField;
  	}

 	public void setNullField(int nullField)
 	{
 		this.nullField = nullField;
 	}

 	public int getSkip()
 	{
 		return this.skip;
 	}

 	public void setSkip(int skip)
 	{
 		this.skip = skip;
 	}

 	public int getRead()
 	{
 		return this.read;
 	}

 	public void setRead(int read)
 	{
 		this.read = read;
 	}

 	public int getRefuse()
 	{
 		return this.refuse;
 	}

 	public void setRefuse(int refuse)
 	{
 		this.refuse = refuse;
 	}

 	public int getAbandon()
 	{
 		return this.abandon;
 	}

 	public void setAbandon(int abandon)
 	{
 		this.abandon = abandon;
 	}

 	public String getStartTime()
 	{
 		return this.startTime;
 	}

 	public void setStartTime(String startTime)
 	{
 		this.startTime = startTime;
 	}

 	public String getEndTime()
 	{
 		return this.endTime;
 	}

 	public void setEndTime(String endTime)
 	{
 		this.endTime = endTime;
 	}
  
 	public double getRunTime()
 	{
 		return this.runTime;
 	}

 	public void setRunTime(double runTime)
 	{
 		this.runTime = runTime;
 	}

 	public List<String> getRuleList()
 	{
 		return this.ruleList;
 	}

 	public void setRuleList(List<String> ruleList)
 	{
 		this.ruleList = ruleList;
 	}
}