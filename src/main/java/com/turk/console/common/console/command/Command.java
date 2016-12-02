package com.turk.console.common.console.command;


public class Command
{
	private String commandString;
	private String commandDescription;
	private CommandAction commandAction;

	public Command(String commandString, String commandDescription, CommandAction commandAction)
	{
		this.commandString = commandString.trim();
		this.commandDescription = commandDescription;
		this.commandAction = commandAction;
	}

	public Command(String commandString, CommandAction commandAction)
	{
		this(commandString, null, commandAction);
	}

	public String getCommandString()
	{
		return this.commandString;
	}

	public void setCommandString(String commandString)
	{
		this.commandString = commandString.trim();
	}

	public String getCommandDescription()
	{
		return this.commandDescription;
	}

	public void setCommandDescription(String commandDescription)
	{
		this.commandDescription = commandDescription;
	}

	public CommandAction getCommandAction()
	{
		return this.commandAction;
	}

	public void setCommandAction(CommandAction commandAction)
	{
		this.commandAction = commandAction;
	}
	
	public boolean equals(Object obj)
	{
		if (obj == this) 
			return true;
		if (obj == null) 
			return false;

		if ((obj instanceof Command))
		{
			Command cmd = (Command)obj;
			return cmd.getCommandString().equals(getCommandAction());
		}
		return false;
	}
}
