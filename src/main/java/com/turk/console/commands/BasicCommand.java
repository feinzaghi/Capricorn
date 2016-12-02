package com.turk.console.commands;

import com.turk.console.common.console.command.CommandAction;
import com.turk.console.common.console.io.CommandIO;

/**
 * ����ָ�������
 * @author Administrator
 *
 */
public abstract class BasicCommand
  implements CommandAction
{
	/**
	 * ָ����
	 */
	public boolean handleCommand(String[] args, CommandIO io)
    	throws Exception
    {
		io.setPrefix(">");
		return doCommand(args, io);
    }

	/**
	 * ִ��ָ��
	 * @param paramArrayOfString
	 * @param paramCommandIO
	 * @return
	 * @throws Exception
	 */
	public abstract boolean doCommand(String[] paramArrayOfString, CommandIO paramCommandIO)
    	throws Exception;
}