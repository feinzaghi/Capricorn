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
			io.println("����,ԭ��:" + e.getMessage());
		}
		String sysStartTime = Util.getDateString(sDate);
		long fast = System.currentTimeMillis() - sDate.getTime();
		String cost = "";

		if (fast < 3600000L)
		{
			cost = Math.round((float)(fast / 60000L)) + " ����";
		}
		else
		{
			cost = Math.round((float)(fast / 3600000L)) + " Сʱ";
		}
		io.println("ϵͳ����ʱ��: " + sysStartTime + "  ������: " + cost);
		return true;
  	}
}