package com.turk.datalog;

import java.util.Date;

public class DataFtpLogInfo {

	private int taskid;
	private Date stamptime;
	private String filename;
	private int deviceid;
	private long filesize;
	
	public int getTaskID()
	{
		return taskid;
	}
	
	public void setTaskID(int TaskID)
	{
		taskid = TaskID;
	}
	
	public Date getStampTime()
	{
		return stamptime;
	}
	
	public void setStampTime(Date StampTime)
	{
		stamptime = StampTime;
	}
	
	public String getFileName()
	{
		return filename;
	}
	
	public void setFileName(String FileName)
	{
		filename = FileName;
	}
	
	public void setDeviceID(int DeviceID)
	{
		deviceid = DeviceID;
	}
	
	public int getDeviceID()
	{
		return deviceid;
	}
	
	public void setFileSize(long FileSize)
	{
		filesize = FileSize;
	}
	
	public long getFileSize()
	{
		return filesize;
	}
}
