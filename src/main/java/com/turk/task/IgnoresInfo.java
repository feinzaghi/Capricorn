package com.turk.task;

public abstract interface IgnoresInfo
{
	public abstract int getTaskId();

	public abstract String getPath();

	public abstract boolean isUsed();

	public abstract void setNotUsed();
}