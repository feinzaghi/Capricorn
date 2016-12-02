package com.turk.alarm;


import java.util.Date;

public class Alarm
{
	private long id;
	private byte alarmLevel;
	private String title;
	private String source;
	private byte status;
	private int sentTimes;
	private String description;
	private int errorCode;
	private int taskID;
  	private Date occuredTime;
  	private Date processedTime;

  	public String getDescription()
  	{
  		return this.description;
  	}

  	public void setDescription(String description)
  	{
  		this.description = removeNoise(description, 1000);
  	}

  	public long getId()
  	{
  		return this.id;
  	}

  	public void setId(long id)
  	{
  		this.id = id;
  	}	

  	/**
  	 * 告警级别
  	 * @return
  	 */
  	public byte getAlarmLevel()
  	{
  		return this.alarmLevel;
  	}

  	public void setAlarmLevel(byte alarmLevel)
  	{
  		this.alarmLevel = alarmLevel;
  	}

  	public String getTitle()
  	{
  		return this.title;
  	}

  	public void setTitle(String title)
  	{
  		this.title = removeNoise(title, 255);
  	}

  	public Date getProcessedTime()
  	{
  		return this.processedTime;
  	}

  	/**
  	 * 告警正文
  	 * @return
  	 */
  	public String getSource()
  	{
  		return this.source;
  	}

  	public void setSource(String source)
  	{
  		this.source = removeNoise(source, 255);
  	}

  	public byte getStatus()
  	{
  		return this.status;
  	}

  	public void setStatus(byte status)
  	{
  		this.status = status;
  	}

  	public Date getOccuredTime()
  	{
  		return this.occuredTime;
  	}

  	public void setOccuredTime(Date occuredTime)
  	{
  		this.occuredTime = occuredTime;
  	}

  	public void setProcessedTime(Date processedTime)
  	{
  		this.processedTime = processedTime;
  	}

  	/**
  	 * 告警错误代码
  	 * @return
  	 */
  	public int getErrorCode()
  	{
  		return this.errorCode;
  	}

  	public void setErrorCode(int errorCode)
  	{
  		this.errorCode = errorCode;
  	}

  	public int getTaskID()
  	{
  		return this.taskID;
  	}

  	public void setTaskID(int taskID)
  	{
  		this.taskID = taskID;
  	}
  	
  	public int getSentTimes()
  	{
  		return this.sentTimes;
 	}

  	public void setSentTimes(int sentTimes)
  	{
  		this.sentTimes = sentTimes;
  	}

  	private String removeNoise(String value, int length)
  	{
  		String reVal = null;
  		if (value != null)
  		{
  			int len = value.length();
  			if (len > length)
  			{
  				value = value.substring(0, 10) + "..." + value.substring(len - (length - 13), len);
  			}
  			reVal = value.replaceAll("[']+", "''");
  		}
  		return reVal;
  	}

  	public static void main(String[] args)
  	{
  		Alarm a = new Alarm();
  		String str = a.removeNoise("select * from aa where time = to_date('2010-05-21 19:00:00','YYYY-MM-DD HH24:MI:SS')", 20);
  		System.out.println(str);
  		System.out.println(str.length());
  	}
}