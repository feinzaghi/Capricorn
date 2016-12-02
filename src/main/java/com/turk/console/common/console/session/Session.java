package com.turk.console.common.console.session;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

import com.turk.console.common.console.command.Command;
import com.turk.console.common.console.command.CommandAction;
import com.turk.console.common.console.io.CommandIO;
import com.turk.console.common.console.util.Util;

/**
 * 控制台会话
 * @author Administrator
 *
 */
public class Session extends Thread
{
	private SessionContext context;
	private CommandIO io;
	private Map<String, Command> commands;
	private CommandAction cmdNotFoundAction;
	private CommandAction loginAction;
	private String welcome;
	private boolean stopFlag = false;
	private int sessionID;

	public Session(SessionContext context, Map<String, Command> commands, CommandAction cmdNotFoundAction, CommandAction loginAction, String welcome)
    	throws IOException
    {
	    this.context = context;
	    this.commands = commands;
	    this.cmdNotFoundAction = cmdNotFoundAction;
	    this.sessionID = Util.creatSessionID();
	    this.loginAction = loginAction;
	    this.welcome = welcome;
	    this.io = new CommandIO(context.getClientSocket().getInputStream(), context.getClientSocket().getOutputStream());
    }

	public synchronized void stopSession()
	{
		this.stopFlag = true;
	}

	public synchronized boolean isStopSession()
	{
		return this.stopFlag;
	}

	public int getSessionID()
	{
		return this.sessionID;
	}

	public SessionContext getSessionContext()
	{
		return this.context;
	}

	public synchronized void start()
	{
		super.start();
		this.context.setBeginTime(new Date());
	}
	
	public void run()
	{
		if (this.welcome == null)
		{
			this.io.println("              ");
		}
		else
		{
			this.io.print(this.welcome);
		}
		boolean isContinue = true;
		if (this.loginAction != null)
		{
			try
			{
				isContinue = this.loginAction.handleCommand(null, this.io);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		do
		{
			String input = null;
			try
			{
				input = this.io.readLine(null);
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			String cmd = null;
			if ((Util.isNotEmptyString(input)) && (input.contains(" ")))
			{
				cmd = input.substring(0, input.indexOf(" "));
			}
			else
			{
				cmd = input.trim();
			}
			if (this.commands.containsKey(cmd))
			{
				boolean b = false;
				try
				{
					b = ((Command)this.commands.get(cmd)).getCommandAction().handleCommand(Util.parseCommandArgs(input), this.io);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
				if (b)
					continue;
				break;
			}

			try
			{
				if (this.cmdNotFoundAction == null)
					continue;
				boolean b = this.cmdNotFoundAction.handleCommand(null, this.io);
				if (b)
				{
					continue;
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
		while ((!isStopSession()) && (isContinue));

		this.io.dispose();
		try
		{
			this.context.getClientSocket().close();
		}
		catch (IOException localIOException1)
		{
		}
		this.context.setEndTime(new Date());
	}
}