package com.turk.alarm;

/**
 * 告警重发规则
 * @author Administrator
 *
 */
public class AlarmReSendRule
{
	private int maxReSendTimes;
	private int timeout;

	public AlarmReSendRule()
	{
	}

	public AlarmReSendRule(int maxReSendTimes, int timeout)
	{
		this.maxReSendTimes = maxReSendTimes;
		this.timeout = timeout;
	}

	public int getMaxReSendTimes()
	{
		return this.maxReSendTimes;
	}

	public void setMaxReSendTimes(int maxReSendTimes)
	{
		this.maxReSendTimes = maxReSendTimes;
	}

	public int getTimeout()
	{
		return this.timeout;
	}

	public void setTimeout(int timeout)
	{
		this.timeout = timeout;
	}
}