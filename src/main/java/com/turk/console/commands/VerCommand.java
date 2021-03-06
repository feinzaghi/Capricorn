package com.turk.console.commands;

import com.turk.config.SystemConfig;
import com.turk.console.common.console.io.CommandIO;

public class VerCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		String edition = SystemConfig.getInstance().getEdition();
		String releaseTime = SystemConfig.getInstance().getReleaseTime();

		io.println(edition + "  " + releaseTime);
		return true;
    }
}