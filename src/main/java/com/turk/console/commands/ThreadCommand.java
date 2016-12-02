package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

import com.turk.util.ThreadPool;

public class ThreadCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		if ((args == null) || (args.length < 1) || 
				(!args[0].trim().toLowerCase().equals("-c")))
		{
			io.println("syntax errors. input help /? for Command Help");
			return true;
		}
		io.println(ThreadPool.getInstance().getRunTask());
		return true;
    }
}