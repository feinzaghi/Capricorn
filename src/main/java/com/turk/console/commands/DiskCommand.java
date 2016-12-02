package com.turk.console.commands;

import java.io.File;

import com.turk.console.common.console.io.CommandIO;

public class DiskCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		try
		{
			File[] roots = File.listRoots();
			for (File f : roots)
			{
				float total = (float)f.getTotalSpace();
				if (total == 0.0F)
					continue;
				float remain = (float)f.getFreeSpace();
				int u = Math.round(remain / total * 100.0F);

				io.println(f.getPath() + "  " + remain / 1.073742E+009F + 
						"GBø…”√   π≤ " + total / 1.073742E+009F + 
						"GB    £”‡: " + u + "%");
			}
		}
		catch (Exception localException)
		{
		}
		return true;
    }
}