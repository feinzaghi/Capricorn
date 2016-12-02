package com.turk.alarm;

public abstract interface RuleFilter
{
	public abstract boolean doFilter(Alarm paramAlarm);
}