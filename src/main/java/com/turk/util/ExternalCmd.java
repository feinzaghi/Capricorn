package com.turk.util;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class ExternalCmd
{
	private String _cmd;

	public void setCmd(String cmd)
	{
		this._cmd = cmd;
	}

	public int execute(String cmd)
		throws Exception
	{
		this._cmd = cmd;
		return execute();
	}

	public int execute()
    	throws Exception
    {
		if (Util.isNull(this._cmd)) {
			return 0;
		}
		int retCode = -1;

		Process proc = null;
		try
		{
			proc = Runtime.getRuntime().exec(this._cmd);
			new StreamGobbler(proc.getErrorStream()).start();
			new StreamGobbler(proc.getInputStream()).start();
			proc.waitFor();
			retCode = proc.exitValue();
		}	
		catch (Exception e)
		{
			throw e;
		}
		finally
		{
    		 if (proc != null) {
    		 proc.destroy();
    		 }
		}
		return retCode;
    }

	class StreamGobbler extends Thread
	{
		InputStream is;

		StreamGobbler(InputStream is) {
			this.is = is;
		}

		public void run()
		{
			BufferedReader br = null;
			try
			{
				br = new BufferedReader(new InputStreamReader(this.is));

				while (br.readLine() != null);
			}
			catch (Exception localException)
			{
				try
				{
					if (br != null)
					{
						br.close();
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
					if (br != null)
					{
						br.close();
					}
				}
				catch (Exception localException2)
				{
				}
			}
		}
	}
}