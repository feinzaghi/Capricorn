package com.turk.alarm;

public abstract interface AlarmSender
{
	public abstract byte send(Alarm paramAlarm);
}