package com.turk.util;

import java.io.PrintStream;
import java.util.Vector;

public class CSVLineParser
{
	public static String[] splitCSV(String src)
	{
		return splitCSV(src, ',');
	}

	public static String[] splitCSV(String src, char splitChar)
	{
		if (src == null)
			throw new NullPointerException();
		StringBuffer st = new StringBuffer();
		Vector result = new Vector();
		boolean beginWithQuote = false;
		for (int i = 0; i < src.length(); i++)
		{
			char ch = src.charAt(i);
			if (ch == '"')
			{
				if (beginWithQuote)
				{
					i++;
					if (i >= src.length())
					{
						result.addElement(st.toString());
						st = new StringBuffer();
						beginWithQuote = false;
					}
					else
					{
						ch = src.charAt(i);
						if (ch == '"')
						{
							st.append(ch);
						} else {
							if (ch != splitChar)
								continue;
							result.addElement(st.toString());
							st = new StringBuffer();
							beginWithQuote = false;
						}
					}
				}
				else
				{
					if (st.length() != 0)
						continue;
					beginWithQuote = true;
				}
			}
			else if (ch == splitChar)
			{
				if (beginWithQuote)
				{
					st.append(ch);
				}
				else
				{
					result.addElement(st.toString());
					st = new StringBuffer();
					beginWithQuote = false;
				}
			}
			else
			{
				st.append(ch);
			}
		}
		if (st.length() != 0)
		{
			if (beginWithQuote)
			{
				throw new RuntimeException("双引号没有正确的结束 \n source:" + src);
			}

			result.addElement(st.toString());
		}

		String[] rs = new String[result.size()];
		for (int i = 0; i < rs.length; i++)
		{
			rs[i] = ((String)result.elementAt(i));
		}
		return rs;
	}
}