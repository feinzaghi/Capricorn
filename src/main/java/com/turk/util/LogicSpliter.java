package com.turk.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.apache.log4j.Logger;

public class LogicSpliter
{
	private Character value;
	private String wrapSigns;
	private boolean keepSign;
	private static final Map<Character, Character> DEFAULT_WRAP_SIGNS = new HashMap();

	private static final Map<Character, Character> USER_WRAP_SIGNS = new HashMap();
	Map<Character, Character> signs;
	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	static
	{
		DEFAULT_WRAP_SIGNS.put(Character.valueOf('"'), Character.valueOf('"'));
		DEFAULT_WRAP_SIGNS.put(Character.valueOf('\''), Character.valueOf('\''));

		DEFAULT_WRAP_SIGNS.put(Character.valueOf('|'), Character.valueOf('|'));
	}

	public LogicSpliter()
	{
		this(null, null, false);
	}

	public LogicSpliter(Character value)
	{
		this(value, null, false);
	}

	public LogicSpliter(Character value, String wrapSigns)
	{
		this(value, wrapSigns, false);
	}

	public LogicSpliter(Character value, String wrapSigns, boolean keepSign)
	{
		this.value = value;
		this.wrapSigns = wrapSigns;
		this.keepSign = keepSign;
		if (wrapSigns != null)
		{
			resolveWrapSigns();
		}
	}

	public Character getValue()
	{
		return this.value;
	}

	public void setValue(Character value)
	{
		this.value = value;
	}

	public String getWrapSigns()
	{
		return this.wrapSigns;
	}

	public void setWrapSigns(String wrapSigns)
	{
		this.wrapSigns = wrapSigns;
	}

	public boolean isKeepSign()
	{
		return this.keepSign;
	}

	public void setKeepSign(boolean keepSign)
	{
		this.keepSign = keepSign;
	}

	public String[] apply(String str)
	{
		if (str == null)
		{
			logger.warn("参数为null");
			return null;
		}

		this.signs = (USER_WRAP_SIGNS.size() == 0 ? DEFAULT_WRAP_SIGNS : USER_WRAP_SIGNS);

		Stack signStack = new Stack();

		char[] chars = ("uway" + this.value + str.toString()).toCharArray();

		List resultList = new ArrayList();

		StringBuilder tempBuffer = new StringBuilder();

		int start = 0;
	    int end = 0;
	    for (int i = 0; i < chars.length; i++)
	    {
	    	char currentChar = chars[i];

	    	if (isSameStartEnd(currentChar))
	    	{
	    		tempBuffer.append(currentChar);
	    	}
	    	else if (isStart(currentChar))
	    	{
	    		StackElement startSignElement = new StackElement()
	    		{
	    			char sign;
	    			int index;

	    			public String toString()
	    			{
	    				return String.format("[sign: %s index: %s]", new Object[] { Character.valueOf(this.sign), Integer.valueOf(this.index) });
	    			}
	    		};
	    		signStack.push(startSignElement);
	    		start = i;
	    	}
	    	else if ((isEnd(currentChar)) && (signStack.size() > 0))
	    	{
	    		StackElement top = (StackElement)signStack.peek();
	    		if (((Character)this.signs.get(Character.valueOf(top.sign))).charValue() != currentChar)
	    			continue;
	    		end = i;
	    		signStack.pop();
	    		if (signStack.size() == 0)
	    		{
	    			resultList.add(subCharArray(chars, top.index, end));
	    			start = 0;
	    		}
	    		end = 0;
	    	}
	    	else
	    	{
	    		if ((start != 0) || (end != 0))
	    			continue;
	    		char preChar = chars[0];

	    		if (currentChar == this.value.charValue())
	    		{
	    			if (((!isStart(preChar)) && (!isEnd(preChar))) || 
	    					(isSameStartEnd(preChar)))
	    			{
	    				resultList.add(tempBuffer.toString());
	    			}
	    			tempBuffer.delete(0, tempBuffer.length());
	    		}
	    		else
	    		{
	    			tempBuffer.append(currentChar);
	    		}
	    	}
	    }

	    if (tempBuffer.length() > 0) {
	    	resultList.add(tempBuffer.toString());
	    }
	    return handleResultList(resultList);
	}
	
	private void resolveWrapSigns()
	{
		String[] splitedPairs = this.wrapSigns.split("|");
		for (String pair : splitedPairs)
		{
			char[] chs = pair.toCharArray();
			if (chs.length != 2) throw new IllegalArgumentException("指定的开始结束符不正确");
			this.signs.put(Character.valueOf(chs[0]), Character.valueOf(chs[1]));
		}
	}

	private boolean isSign(char ch, boolean isStart)
	{
		Iterator signsIterator = this.signs.entrySet().iterator();

		while (signsIterator.hasNext())
		{
			Map.Entry current = (Map.Entry)signsIterator.next();
			if (isStart)
			{
				if (ch == ((Character)current.getKey()).charValue()) return true;
			}
			else if (ch == ((Character)current.getValue()).charValue()) return true;
		}
		return false;
	}

	private boolean isStart(char ch)
	{
		return isSign(ch, true);
	}

	private boolean isEnd(char ch)
	{
		return isSign(ch, false);
	}

	private boolean isSameStartEnd(char ch)
	{
		Character value = (Character)this.signs.get(Character.valueOf(ch));
		return (value != null) && (value.charValue() == ch);
	}

	private String subCharArray(char[] charArray, int start, int end)
	{
		StringBuilder buffer = new StringBuilder();
		for (int i = start; i <= end; i++)
		{
			buffer.append(charArray[i]);
		}
		if (!this.keepSign)
		{
			trimSign(buffer);
		}
		return buffer.toString();
	}

	private void trimSign(StringBuilder buffer)
	{
		if (isStart(buffer.charAt(0)))
		{
			buffer.delete(0, 1);
		}
		if (isEnd(buffer.charAt(buffer.length() - 1)))
		{
			buffer.delete(buffer.length() - 1, buffer.length());
		}
	}

 	private String[] handleResultList(List<String> resultList)
 	{
 		List handledList = new ArrayList();

 		int start = 0;
 		int end = 0;
 		boolean started = false;
 		for (int i = 0; i < resultList.size(); i++)
 		{
 			String field = (String)resultList.get(i);
 			if ((isSameStartEnd(field.length() == 0 ? '\000' : field.charAt(0))) && 
 					(!started))
 			{
 				start = i;
 				started = true;
 			}
 			if ((!isSameStartEnd(field.length() == 0 ? '\000' : field.charAt(field.length() - 1))) || 
 					(!started))
 				continue;
 			end = i;
 		}

 		if (start == 0)
 		{
 			if (resultList.size() > 0)
 			{
 				resultList.remove(0);
 			}
 			return (String[])resultList.toArray(new String[0]);
 		}

 		StringBuilder buffer = new StringBuilder();
 		for (int i = start; i <= end; i++)
 		{
 			buffer.append((String)resultList.get(i));
 			if (i == end)
 				continue;
 			buffer.append(this.value);
 		}

 		if (!this.keepSign)
 		{
 			trimSign(buffer);
 		}

 		boolean inseted = false;
 		for (int i = 0; i < resultList.size(); i++)
 		{
 			if ((i < start) || (i > end))
 			{
 				handledList.add((String)resultList.get(i));
 			} else {
 				if (inseted)
 					continue;
 				handledList.add(buffer.toString());
 				inseted = true;
 			}
 		}
 		if (handledList.size() > 0)
 		{
 			handledList.remove(0);
 		}
 		return (String[])handledList.toArray(new String[0]);
 	}

 	public static void main(String[] args)
 	{
 		String data = "sfds,,\"sdfs,<sdf,ok>,sdfsdfd,<2<33,3324>,sdf>,rwrwe";
 		LogicSpliter lo = new LogicSpliter(Character.valueOf(','));
 		lo.setKeepSign(true);
 		String[] ss = lo.apply(data);
 		for (String s : ss)
 		{
 			System.out.println(s);
 		}
  	}
}

class StackElement
{
	char sign;
    int index;
}