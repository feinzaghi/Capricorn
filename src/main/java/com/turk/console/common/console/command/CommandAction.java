package com.turk.console.common.console.command;

import com.turk.console.common.console.io.CommandIO;

public abstract interface CommandAction
{
	public abstract boolean handleCommand(String[] paramArrayOfString, CommandIO paramCommandIO)
    	throws Exception;
}