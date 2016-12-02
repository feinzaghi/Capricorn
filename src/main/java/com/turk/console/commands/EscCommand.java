package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

public class EscCommand extends BasicCommand{

	@SuppressWarnings("deprecation")
	@Override
	public boolean doCommand(String[] paramArrayOfString,
			CommandIO paramCommandIO) throws Exception {
		RateCommand.thread.stop();
		return true;
	}

}
