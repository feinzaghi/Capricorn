package com.turk.console.commands;

import java.util.Date;

import com.turk.console.common.console.io.CommandIO;

import com.turk.util.Util;

public class SysCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		Date sDate = null;
		try
		{
			sDate = (Date)Class.forName("framework.Capricorn").getDeclaredField("SYS_START_TIME").get(null);
		}
		catch (Exception e)
		{
			io.println("错误,原因:" + e.getMessage());
		}
		String sysStartTime = Util.getDateString(sDate);
		long fast = System.currentTimeMillis() - sDate.getTime();
		String cost = "";

		if (fast < 3600000L)
		{
			cost = Math.round((float)(fast / 60000L)) + " 分钟";
		}
		else
		{
			cost = Math.round((float)(fast / 3600000L)) + " 小时";
		}
		io.println("系统启动时间: " + sysStartTime + "  已运行: " + cost);
		return true;
  	}
}