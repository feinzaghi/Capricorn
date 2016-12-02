package com.turk.console;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.turk.console.commands.DateCommand;
import com.turk.console.commands.DiskCommand;
import com.turk.console.commands.ErrorCommand;
import com.turk.console.commands.ExitCommand;
import com.turk.console.commands.HelpCommand;
import com.turk.console.commands.HostCommand;
import com.turk.console.commands.JvmCommand;
import com.turk.console.commands.KillCommand;
import com.turk.console.commands.ListCommand;
import com.turk.console.commands.MasterCommand;
import com.turk.console.commands.NullCommand;
import com.turk.console.commands.OsCommand;
import com.turk.console.commands.SlaveCommand;
import com.turk.console.commands.StopCommand;
import com.turk.console.commands.SysCommand;
import com.turk.console.commands.TaurusmonitorCommand;
import com.turk.console.commands.ThreadCommand;
import com.turk.console.commands.VerCommand;
import com.turk.console.common.console.command.Command;
/**
 * 控制台管理
 * @author Administrator
 *
 */
public final class ConsoleMgr
{
	private com.turk.console.common.console.Console console;
	private static ConsoleMgr instance;
	private static String WELCOME_INFO = "  ----------------------------------------------------\r\n                  Welcome to CapricornV2              \r\n       Copyright @ Utele All Rights Reserved   \r\n  ----------------------------------------------------\r\n";

	private ConsoleMgr(int port)
	{
		//int port = SystemConfig.getInstance().getCollectPort();
	    Map<String, Command> cmds = new HashMap<String, Command>();
	    cmds.put("?", new Command("?", new HelpCommand()));
	    cmds.put("help", new Command("help", new HelpCommand()));
	    cmds.put("date", new Command("date", new DateCommand()));
	    cmds.put("disk", new Command("disk", new DiskCommand()));
	    cmds.put("error", new Command("error", new ErrorCommand()));
	    cmds.put("exit", new Command("exit", new ExitCommand()));
	    cmds.put("host", new Command("host", new HostCommand()));
	    cmds.put("jvm", new Command("jvm", new JvmCommand()));
	    cmds.put("kill", new Command("kill", new KillCommand()));
	    cmds.put("list", new Command("list", new ListCommand()));
	    cmds.put("os", new Command("os", new OsCommand()));
	    cmds.put("stop", new Command("stop", new StopCommand()));
	    cmds.put("sys", new Command("sys", new SysCommand()));
	    cmds.put("thread", new Command("thread", new ThreadCommand()));
	    cmds.put("ver", new Command("ver", new VerCommand()));
	    cmds.put("taurus", new Command("taurus", new TaurusmonitorCommand()));
	    cmds.put("master", new Command("master", new MasterCommand()));
	    cmds.put("slave", new Command("slave", new SlaveCommand()));
	    //cmds.put("dttask", new Command("dttask", new TaskCommand()));
	    //cmds.put("cpu", new Command("cpu", new RateCommand()));
	    //cmds.put("esc", new Command("esc", new EscCommand()));
	    //cmds.put("threadname", new Command("threadname", new ThreadNameCommand()));
	    cmds.put("", new Command("", new NullCommand()));
	    
	    try
	    {
	    	this.console = new com.turk.console.common.console.Console(port, cmds, new NotFindAction(), new LoginAction(), WELCOME_INFO);
	    }
	    catch (IOException e)
	    {
	    	e.printStackTrace();
	    }
	}

	public static synchronized ConsoleMgr getInstance(int port)
	{
		if (instance == null)
		{
			instance = new ConsoleMgr(port);
		}
		return instance;
	}

	public void start()
	{
		if (this.console != null)
    	{
			try
			{
				this.console.start();
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
    	}
	}

	public void stop()
	{
		if (this.console != null)
		{
			this.console.stop();
		}
	}

	//public static void main(String[] args)
	//{
		//getInstance().start();
	//}
}