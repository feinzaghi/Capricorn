package com.turk.console.common.console;

import java.io.IOException;
import java.util.Map;

import com.turk.console.common.console.command.Command;
import com.turk.console.common.console.command.CommandAction;
import com.turk.console.common.console.io.ConsoleSocketServer;

/**
 * ¿ØÖÆÌ¨
 * @author Administrator
 *
 */
public class Console extends ConsoleSocketServer
{
	public Console(int port, Map<String, Command> commands, CommandAction cmdNotFoundAction, String welcome)
		throws IOException
    {
		super(port, commands, cmdNotFoundAction, null, welcome);
    }

	public Console(int port, Map<String, Command> commands, CommandAction cmdNotFoundAction, CommandAction loginAction, String welcome)
    	throws IOException
    {
		super(port, commands, cmdNotFoundAction, loginAction, welcome);
    }

	public Console(int port, Map<String, Command> commands, CommandAction cmdNotFoundAction)
		throws IOException
	{
		this(port, commands, cmdNotFoundAction, null);
	}

	public Console(int port, Map<String, Command> commands)
		throws IOException
	{
		this(port, commands, null, null);
	}
}
