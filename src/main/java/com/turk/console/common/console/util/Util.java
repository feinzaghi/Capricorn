package com.turk.console.common.console.util;

import java.util.ArrayList;
import java.util.List;

public final class Util
{
	

	private static int sessionID = 1000;

	public static boolean isEmptyString(String str)
	{
		if (str == null) return true;
		String s = str.trim();
		return s.equals("");
	}

	public static boolean isNotEmptyString(String str)
	{
		return !isEmptyString(str);
	}

	public static synchronized void debug(Object obj)
	{
	}

	public static String[] parseCommandArgs(String cmd)
	{
		if (isEmptyString(cmd)) return null;
		if (!cmd.trim().contains(" ")) return null;

		List<String> tmp = new ArrayList<String>();
		String[] ss = cmd.split(" ");
		for (String s : ss)
		{
			if (!isNotEmptyString(s))
				continue;
			tmp.add(s.trim());
		}

		if (tmp.size() > 0)
		{
			tmp.remove(0);
		}
		if (tmp.size() == 0)
		{	
			return null;
		}

		return (String[])tmp.toArray(new String[0]);
	}

	public static synchronized int creatSessionID()
	{
		return sessionID++;
	}

	public static void main(String[] args)
	{
		String[] a = parseCommandArgs("ls ");
    	System.out.println(a);
	}
}
