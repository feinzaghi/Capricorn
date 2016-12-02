package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

public class ExitCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		io.println("Bye.");
		return false;
    }
}