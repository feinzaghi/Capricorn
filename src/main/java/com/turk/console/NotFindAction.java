package com.turk.console;

import com.turk.console.common.console.command.CommandAction;
import com.turk.console.common.console.io.CommandIO;

class NotFindAction
  	implements CommandAction
{
	public boolean handleCommand(String[] args, CommandIO io)
    	throws Exception
    {
		io.setPrefix(">");
		io.println("�������������ڣ�������help��?��ȡ����");
		return true;
    }
}