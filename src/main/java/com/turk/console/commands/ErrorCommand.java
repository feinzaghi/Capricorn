package com.turk.console.commands;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

import com.turk.console.common.console.io.CommandIO;

public class ErrorCommand extends BasicCommand
{
	public boolean doCommand(String[] args, CommandIO io)
    	throws Exception
    {
		String stdErrFile = "." + File.separator + "log" + File.separator + 
			"error.log";
		File fError = new File(stdErrFile);
		if ((fError.exists()) && (fError.isFile()))
		{
			BufferedReader br = null;
			try
			{
				br = new BufferedReader(new InputStreamReader(new FileInputStream(fError)));
				String strLine = null;
				while ((strLine = br.readLine()) != null)
				{
					io.println(strLine);
				}
			}
			catch (FileNotFoundException e)
			{
				io.println("��׼������ļ� " + stdErrFile + " ������");

				if (br == null) 
					return true;
				try {
					br.close();
				}
				catch (IOException localIOException1)
				{
				}
			}
			catch (IOException e)
			{
				io.println("��ȡ��׼�������Ϣʱ�쳣,ԭ��: " + e.getMessage());

				if (br == null) 
					return true;
				try {
					br.close();
				}
				catch (IOException localIOException2)
				{
				}
			}
			finally
			{
				if (br != null)
				{
					try
					{
						br.close(); 
					} 
					catch (IOException localIOException3) {
						} 
				}
			}
			
			try { 
				br.close();
			}catch (IOException localIOException4)
			{
			}
		}
		else
		{
			io.println("��׼������ļ� " + stdErrFile + " ������");
		}
    
		return true;
    }
}