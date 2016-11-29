package com.turk.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;

public class XMLConvertAL
{
	public static File convert(String fileName)
		throws Exception
	{
		return convert(new File(fileName));
	}

	public static File convert(File file) throws Exception
	{
		FileInputStream fis = null;
		BufferedReader br = null;
		InputStreamReader isr = null;
    	File result = new File(file.getAbsoluteFile() + ".tmp");
    	PrintWriter pw = null;
    	try
    	{
    		pw = new PrintWriter(result);
    		fis = new FileInputStream(file);
    		isr = new InputStreamReader(fis);
    		br = new BufferedReader(isr);
    		StringBuilder buffer = new StringBuilder();
    		boolean cleared = false;
    		char[] cs = new char[65535];
    		while (br.read(cs) > -1)
    		{
    			buffer.append(cs);
    			if (!cleared)
    			{
    				int right = buffer.indexOf("?>");
    				buffer.insert(right + 2, '\n');
    				int mdc = buffer.indexOf("<mdc");
    				buffer.delete(right + 3, mdc);
    				cleared = true;
    			}
    			pw.print(buffer.toString().trim());
    			pw.flush();
    			buffer.delete(0, buffer.length());
    		}

    	}
    	catch (Exception localException)
    	{
    		try
    		{
    			if (fis != null)
    			{
    				fis.close();
    			}
    			if (isr != null)
    			{
    				isr.close();
    			}
    			if (br != null)
    			{
    				br.close();
    			}
    			if (pw != null)
    			{
    				pw.flush();
    				pw.close();
    			}
    		}
    		catch (Exception localException1)
    		{
    		}
    	}
    	finally
    	{
    		try
    		{
    			if (fis != null)
    			{
    				fis.close();
    			}
    			if (isr != null)
    			{
    				isr.close();
    			}
    			if (br != null)
    			{
    				br.close();
    			}
    			if (pw != null)
    			{
    				pw.flush();
    				pw.close();
    			}
    		}
    		catch (Exception localException2)
    		{
    		}
    	}
    	if (!file.delete())
    		throw new Exception("Դ�ļ�ɾʧ�ܣ����ܱ�ռ��:" + file.getAbsolutePath());
    	if (!result.renameTo(file))
    	{
    		throw new Exception("����ʱ�ļ�����ʱ�쳣:" + result.getAbsolutePath() + 
    				" rename to " + file.getAbsolutePath());
    	}

    	result = file;

    	return result;
	}

	public static void main(String[] args)
	{
		try
		{
			File f = convert("C:\\Users\\ChenSijiang\\Desktop\\A20100801.0300+0800-0400+0800_NodeB-DQ1_0417rangqulishuisanqiW_BOB");
			System.out.println(f.getAbsolutePath());
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
}