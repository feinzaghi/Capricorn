package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

import com.turk.util.Util;

/**
 * ��ʾ��������
 * @author Administrator
 *
 */
public class HostCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		io.println(Util.getHostName());
		return true;
    }
}