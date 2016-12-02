package com.turk.console.commands;

import com.turk.console.common.console.command.CommandAction;
import com.turk.console.common.console.io.CommandIO;

/**
 * 基础指令抽象类
 * @author Administrator
 *
 */
public abstract class BasicCommand
  implements CommandAction
{
	/**
	 * 指令句柄
	 */
	public boolean handleCommand(String[] args, CommandIO io)
    	throws Exception
    {
		io.setPrefix(">");
		return doCommand(args, io);
    }

	/**
	 * 执行指令
	 * @param paramArrayOfString
	 * @param paramCommandIO
	 * @return
	 * @throws Exception
	 */
	public abstract boolean doCommand(String[] paramArrayOfString, CommandIO paramCommandIO)
    	throws Exception;
}