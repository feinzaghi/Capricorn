package com.turk.console.commands;

import java.util.Properties;

import com.turk.console.common.console.io.CommandIO;

public class OsCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		Properties props = System.getProperties();
		String osName = props.getProperty("os.name");
		String osArch = props.getProperty("os.arch");
		String osVersion = props.getProperty("os.version");
		io.println(osName + "  " + osArch + "  " + osVersion);
		return true;
    }
}