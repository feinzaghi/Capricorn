package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

public class NullCommand extends BasicCommand
{
  public boolean doCommand(String[] args, CommandIO io)
    throws Exception
  {
    return true;
  }
}