package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

public class JvmCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		float maxMemory = (float)(Runtime.getRuntime().maxMemory() / 1048576L);
		float totalMemory = (float)(Runtime.getRuntime().totalMemory() / 1048576L);
		float freeMemory = (float)(Runtime.getRuntime().freeMemory() / 1048576L);
		float usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;

		io.println("jvm memory usage: ");
		io.println("已使用: " + usedMemory + "M  剩余: " + freeMemory + "M  最大内存: " + 
				maxMemory + "M");
		return true;
    }
}