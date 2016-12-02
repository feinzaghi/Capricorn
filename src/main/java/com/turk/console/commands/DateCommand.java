package com.turk.console.commands;

import java.util.Date;

import com.turk.console.common.console.io.CommandIO;
import com.turk.util.Util;

public class DateCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		io.println(Util.getDateString(new Date()));
		return true;
    }
}