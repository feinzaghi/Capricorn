package com.turk.db.pojo;

public class CollectLog
{
	private String logTime;
	private int taskId;
	private String taskDescription;
	private String taskType;
	private String taskStatus;
	private String dataTime;
	private int costTime;
	private String taskResult;
	private String taskDetail;
	private String taskException;
	private String dataEndTime;

	public String getLogTime()
	{
		return this.logTime;
	}

	public void setLogTime(String logTime) {
		this.logTime = logTime;
	}

	public int getTaskId() {
		return this.taskId;
	}

	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}

	public String getTaskDescription() {
		return this.taskDescription;
	}

	public void setTaskDescription(String taskDescription) {
		this.taskDescription = taskDescription;
	}

	public String getTaskType() {
		return this.taskType;
	}

	public void setTaskType(String taskType) {
		this.taskType = taskType;
	}

	public String getTaskStatus() {
		return this.taskStatus;
	}

	public void setTaskStatus(String taskStatus) {
		this.taskStatus = taskStatus;
	}

	public String getDataTime() {
		return this.dataTime;
	}

	public void setDataTime(String dataTime) {
		this.dataTime = dataTime;
	}

	public int getCostTime() {
		return this.costTime;
	}

	public void setCostTime(int costTime) {
		this.costTime = costTime;
	}

	public String getTaskResult() {
		return this.taskResult;
	}

	public void setTaskResult(String taskResult) {
		this.taskResult = taskResult;
	}

	public String getTaskDetail() {
		return this.taskDetail;
	}

	public void setTaskDetail(String taskDetail) {
		this.taskDetail = taskDetail;
	}

	public String getTaskException() {
		return this.taskException;
	}

	public void setTaskException(String taskException) {
		this.taskException = taskException;
	}

	public String getDataEndTime() {
		return this.dataEndTime;
	}

	public void setDataEndTime(String dataEndTime) {
		this.dataEndTime = dataEndTime;
	}
}