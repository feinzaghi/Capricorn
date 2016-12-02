package com.turk.console.common.console.io;


import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

import com.turk.console.common.console.util.Util;

/**
 * ÃüÁîIO
 * @author Administrator
 *
 */
public class CommandIO
{
	private InputStream in;
	private PrintWriter pw;
	private OutputStream out;
	private String prefix;
	private String leftPadding;
	
	public CommandIO(InputStream in, OutputStream out)
	{
		this.in = in;
		this.out = out;
		this.pw = new PrintWriter(out);
		
	}

	//public void println(String s)
	//{
	//	this.pw.print((this.leftPadding != null ? this.leftPadding + s : s) + newline);
	//	this.pw.flush();
	//}
	
	public void println(String s)
	{
		this.pw.print((this.leftPadding != null ? this.leftPadding + s : s) + " \r\n");
		this.pw.flush();
	}

	public void print(String s)
	{
		this.pw.print(s);
		this.pw.flush();
	}

	public String getPrefix()
	{
		return this.prefix;
	}

	public void setPrefix(String prefix)
	{
		this.prefix = prefix;
	}

	public OutputStream getSourceOutputStream()
	{
		return this.out;
	}

	public String readLine(String tip, boolean echo)
    	throws IOException
    {
		if (this.prefix != null)
		{
			print(this.prefix);
		}
		if (tip != null)
		{
			print(tip);
		}

		StringBuilder buffer = new StringBuilder();
		int i = -1;
		
		while (((i = this.in.read()) > -1) && (i != 10))
		{
			if ((!echo) && (i != 8))
			{
				
				//maskChar();
			}
			if ((i != 8) && (i != 13) && 
					(i != 10))
			{
				buffer.append((char)i);
				if ((echo) || (i == 8))
					continue;
				//String last = tip != null ? tip : this.prefix;
				//if (last != null)
				//{
				//	last = last.substring(last.length() - 1, last.length());
				//	print(last);
				//}
				//print(" ");
				//backspace();
				maskChar();
			}
			else {
				if (i != 8)
					continue;
				if ((!echo) && (buffer.length() > 0))
				{
					buffer.deleteCharAt(buffer.length() - 1);
				}
				if ((buffer.length() > 0) && (echo))
				{
					backspace();
					buffer.deleteCharAt(buffer.length() - 1);
				}
				else
				{
					String last = tip != null ? tip : this.prefix;
					if (last != null)
					{
						last = last.substring(last.length() - 1, last.length());
						print(last);
					}
					print("");
				}
			}
		}
		String s = buffer.toString().trim();
		Util.debug(s);
		return s;
    }

	@Deprecated
  	public String readLine_(String tip, boolean echo)
    	throws IOException
    {
	  	if (this.prefix != null)
	  	{
    		print(this.prefix);
    	}
    	if (tip != null)
    	{
    		print(tip);
    	}
    	if (!echo)
    	{
    		printNull();
    	}

    	StringBuilder buffer = new StringBuilder();

    	int i = -1;

    	while (((i = this.in.read()) != 13) && (i > -1))
    	{
    		if (!echo)
      		{
    			print("");
        		maskChar();
      		}

      		if ((i != 8) && (i != 10))
      		{
    	  		buffer.append((char)i);
      		}
      		else if ((buffer.length() > 0) && 
    		  	(buffer.charAt(buffer.length() - 1) != '\n'))
      		{
		        backspace();
		        buffer.deleteCharAt(buffer.length() - 1);
      		}
      		else if ((i != 10) && (i != 8))
      		{
      			printNull();
      		}
      		else if ((i == 8) && (buffer.length() < 1) && 
      			(this.prefix != null))
      		{
    	  		printNull();
      		}
      		else
      		{
    	  		if (buffer.length() != 0)
    	  			continue;
    	  		buffer.append('\n');
      		}

    	}

    	if (buffer.length() == 0) 
    			return null;

	    String str = buffer.toString().trim();
	    buffer.delete(0, buffer.length());
	    buffer = null;
	    return str;
  	}

  	public String readLine(String tip)
  		throws IOException
    {
	  return readLine(tip, true);
  	}

  	public InputStream getSourceInputStream()
  	{
  		return this.in;
  	}

  	public void dispose()
  	{
	    try
	    {
	    	this.in.close();
	    }
	    catch (IOException localIOException)
	    {
	    }
	    this.pw.close();
	    try
	    {
	    	this.out.close();
	    }
	    catch (IOException localIOException1)
	    {
	    }
	    this.in = null;
	    this.pw = null;
	    this.out = null;
	    this.prefix = null;
  	}

  	public String getLeftPadding()
  	{
  		return this.leftPadding;
  	}

  	public void setLeftPadding(String leftPadding)
  	{	
  		this.leftPadding = leftPadding;
  	}

  	private void maskChar()
  	{
  		this.pw.print("\b");  	
  		this.pw.print("*");
  		this.pw.flush();
  	}

  	private void printNull()
  	{
  		this.pw.print(' ');
  		this.pw.flush();
  	}

  	private void backspace()
  	{
  		this.pw.print(' ');
  		this.pw.print('\b');
  		this.pw.flush();
  	}
}
