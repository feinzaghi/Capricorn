package com.turk.util.string;

import java.util.StringTokenizer;
import java.util.Vector;

public final class StringArrayHelper
{
	public static final int ASCENDING = 1;
	public static final int DESCENDING = -1;

	public static String[] addStringArrays(String[] dataA, String[] dataB)
	{
		String[] data = new String[dataA.length + dataB.length];
    
		System.arraycopy(dataA, 0, data, 0, dataA.length);
		System.arraycopy(dataB, 0, data, dataA.length, dataB.length);

		return data;
	}

	public static String[] addStringToArray(String dataA, String[] dataB)
	{
		String[] data = new String[dataB.length + 1];

		System.arraycopy(dataB, 0, data, 0, dataB.length);
		data[(data.length - 1)] = dataA;

		return data;
	}

	public static String arrayToString(String[] data, String delim)
	{
		return arrayToString(data, delim, 0, data.length);
	}

	public static String arrayToString(String[] data, String delim, int start, int end)
	{
		StringBuffer st = new StringBuffer();

		for (int loop = start; loop < end; loop++)
		{
			if (loop != start)
				st.append(delim);
			st.append(data[loop]);
		}
		
		return st.toString();
	}

	public static String[] parseFields(String source, char delimeter)
	{
		return parseFields(source, delimeter, '"', '"');
	}

	public static String[] parseFields(String source, char delimeter, char startIgnor, char endIgnor)
	{
		if (source == null) {
			return new String[0];
		}
		StringBuffer token = new StringBuffer();
		Vector tokens = new Vector();

		int insideLevel = 0;
		for (int loop = 0; loop < source.length(); loop++)
		{
			char c = source.charAt(loop);
			
			if ((c == startIgnor) && (insideLevel == 0))
			{
				insideLevel++;
			}
			else if (c == endIgnor)
			{
				insideLevel--;

				if (startIgnor == endIgnor)
				{
					insideLevel--;
				}

				if (insideLevel < 0)
				{
					insideLevel = 0;
				}
			}

			if ((insideLevel == 0) && (c == delimeter))
			{	
				tokens.addElement(token);
				token = new StringBuffer();
			}
			else
			{
				token.append(source.charAt(loop));
			}
		}

		if (token.length() > 0)
		{
			tokens.addElement(token);
		}

		return vectorToStringArray(tokens);
	}

	public static String[] parseFields(String source, String delimiters)
	{
		StringTokenizer tokenizer = new StringTokenizer(source, delimiters);

		String[] data = new String[tokenizer.countTokens()];
		int loop = 0;
		while (tokenizer.hasMoreTokens())
		{
			data[loop] = tokenizer.nextToken();
			loop++;
		}

		return data;
	}

	public static String[] parseSpacedFields(String source)
	{
		if (source == null) {
			return new String[0];
		}
		StringBuffer token = new StringBuffer();
		Vector tokens = new Vector();

		boolean insideQuotes = false;
		for (int loop = 0; loop < source.length(); loop++)
		{
			char c = source.charAt(loop);

			if ((c == '"') || (c == '\''))
			{
				if (insideQuotes)
					insideQuotes = false;
				else {
					insideQuotes = true;
				}
			}
			if ((!insideQuotes) && (c == ' '))
			{
				tokens.addElement(token);
				token = new StringBuffer();
			}
			else
			{
				token.append(source.charAt(loop));
			}
		}

		if (token.length() > 0) {
			tokens.addElement(token);
		}
		return vectorToStringArray(tokens);
	}

	public static String[] stringToArray(String data)
	{
		Vector arrayV = new Vector();

		StringBuffer buffer = new StringBuffer();

		for (int loop = 0; loop < data.length(); loop++)
		{
			switch (data.charAt(loop))
			{
				case '\n':
					arrayV.addElement(buffer.toString());
					buffer = new StringBuffer();
					break;
				case '\r':
					break;
				case '\t':
					buffer.append("    ");
					break;
				case '\013':
				case '\f':
				default:
					buffer.append(data.charAt(loop));
			}
		}

		if (buffer.length() > 0) {
			arrayV.addElement(buffer.toString());
		}
		String[] array = new String[arrayV.size()];
		for (int loop = 0; loop < arrayV.size(); loop++)
		{
			array[loop] = ((String)arrayV.elementAt(loop));
		}

		return array;
	}

	public static String[] vectorToStringArray(Vector data)
	{	
		String[] array = new String[data.size()];

		for (int loop = 0; loop < data.size(); loop++)
		{
			array[loop] = data.elementAt(loop).toString();
		}

		return array;
	}

	public static String[] stripDoubleQuotes(String[] src)
	{
		for (int loop = 0; loop < src.length; loop++)
		{
			if ((src[loop].length() <= 2) || (src[loop].charAt(0) != '"') || 
					(src[loop].charAt(src[loop].length() - 1) != '"'))
				continue;
			src[loop] = src[loop].substring(1, src[loop].length() - 1);
		}

		return src;
	}

	public static String[] removeEmptyStrings(String[] src)
	{
		int emptyCount = 0;
		for (int loop = 0; loop < src.length; loop++)
		{
			if (src[loop].length() != 0)
				continue;
			emptyCount++;
		}

		if (emptyCount == 0) return src;

		String[] target = new String[src.length - emptyCount];
		int entry = 0;
    
		for (int loop = 0; loop < src.length; loop++)
		{
			if (src[loop].length() <= 0)
				continue;
			target[entry] = src[loop];
			entry++;
		}

		return target;
	}

	public static final String[] sort(String[] table, int direction)
	{
		return applyIndex(table, createIndex(table, direction));
	}

	public static final String[] applyIndex(String[] table, int[] index)
	{
		int size = table.length;
		if (index.length != size) {
			throw new ArrayIndexOutOfBoundsException();
		}
		String[] tempTable = new String[table.length];
    
		for (int loop = 0; loop < size; loop++) {
			tempTable[loop] = table[index[loop]];
		}
		return tempTable;
	}

	public static final int[] createIndex(String[] table, int direction)
	{
		int size = table.length;

		if (size == 0) {
			return new int[0];
		}
		if ((direction != 1) && (direction != -1)) {
			return new int[table.length];
		}
		int[] index = new int[size];
		boolean[] indexed = new boolean[size];

		for (int loop = 0; loop < size; loop++) {
			indexed[loop] = false;
   	 	}

		for (int loop = 0; loop < size; loop++)
		{
			int foundIndex = 0;
			String foundObj = null;

			for (int loop2 = 0; loop2 < size; loop2++)
			{
				if (indexed[loop2] != false) {
					continue;
				}
				if (foundObj == null)
				{
					foundObj = table[loop2];
					foundIndex = loop2;
				}
				else if (direction == 1)
				{
					if (foundObj.compareTo(table[loop2]) <= 0)
						continue;
					foundObj = table[loop2];
					foundIndex = loop2;
				}
				else
				{
					if (foundObj.compareTo(table[loop2]) >= 0)
						continue;
					foundObj = table[loop2];
					foundIndex = loop2;
				}

			}
			indexed[foundIndex] = true;
			index[loop] = foundIndex;
		}
		return index;
	}

	public static Vector stringArrayToVector(String[] data)
	{
		Vector vData = new Vector();
		for (int loop = 0; loop < data.length; loop++)
		{
			if ((data[loop].length() > 0) && 
					(data[loop].charAt(data[loop].length() - 1) == '\r'))
			{
				data[loop] = data[loop].substring(0, data[loop].length() - 1);
			}
			vData.addElement(data[loop]);
		}

		return vData;
	}	
}