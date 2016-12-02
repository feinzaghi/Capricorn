package com.turk.collect;

import java.io.InputStream;
import java.io.PrintStream;

import org.apache.commons.net.telnet.TelnetClient;
import org.apache.log4j.Logger;

import com.turk.alarm.AlarmMgr;

import com.turk.util.LogMgr;

public class ProtocolTelnet
{
	private InputStream in;
	private PrintStream out;
	private TelnetClient client;
	private boolean proxy = false;

	private Logger log = LogMgr.getInstance().getSystemLogger();

	public void Close()
	{
		try
		{
			if (this.proxy) {
				sendCmd("exit");
			}
			if (this.in != null) {
				this.in.close();
			}
			if (this.out != null) {
				this.out.close();
			}
			if ((this.client != null) && (this.client.isConnected()))
				this.client.disconnect();
		}
		catch (Exception e)
		{
			this.log.error("Telnet> Close error.", e);
		}
	}

	public boolean sendCmd(String strCmd)
	{
		boolean bResult = true;
		try
		{
			this.out.println(strCmd);
			this.out.flush();
		}
		catch (Exception e)
    	{
			this.log.error("Telnet> sendCmd error. " + strCmd, e);
			bResult = false;
    	}
		return bResult;
	}

	public boolean ExecuteShell(String strShell, String strEnd, int TimeOut)
	{
		boolean bResult = false;
		try
		{
			sendCmd(strShell);
			bResult = waitForString(strEnd, TimeOut);
		}
		catch (Exception e)
		{
			this.log.error("Telnet> ExecuteShell error.", e);
		}

		return bResult;
	}

	public boolean ProxyLogin(String strHost, int nPort, String strUser, String strPwd, String strSign, String strTermType)
	{
		try
		{
			this.proxy = true;
			if (this.client.isConnected())
			{
				if (this.in == null) {
					return false;
				}

				String strConnect = String.format("telnet %s %d", new Object[] { strHost, Integer.valueOf(nPort) });
				sendCmd(strConnect);

				if (waitForString("ogin:", 5000L))
					sendCmd(strUser);
				else {
					return false;
				}
				if (waitForString("assword:", 5000L))
					sendCmd(strPwd);
				else {
					return false;
				}
				if (!waitForString(strSign, 5000L)) {
					return false;
				}
			}
			else
			{
				return false;
			}
		}
		catch (Exception e)
		{
			this.log.error("Telnet> ProxyLogin error.", e);
			return false;
		}
		return true;
	}

	public boolean Login(String strHost, int nPort, String strUser, String strPwd, String strSign, String strTermType, int nTimeOut)
	{
		try
		{
			this.proxy = false;
			this.client = new TelnetClient(strTermType);
      		this.client.setReaderThread(true);
      		this.client.connect(strHost, nPort);
      		this.in = this.client.getInputStream();
      		this.out = new PrintStream(this.client.getOutputStream());

      		this.client.setSoTimeout(nTimeOut * 1000);

      		if (this.in == null) {
      			return false;
      		}
      		if (waitForString("ogin:", 5000L))
      			sendCmd(strUser);
      		else {
      			return false;
      		}
      		if (waitForString("assword:", 5000L))
      			sendCmd(strPwd);
      		else {
      			return false;
      		}
      		if (!waitForString(strSign, 5000L)) {
      			return false;
      		}
		}
		catch (Exception e)
		{
			this.log.error("Telnet> login error.", e);
			return false;
		}
		return true;
	}

	public int readData(byte[] buffer)
	{
		int nLen = -1;
		try
		{
			if (this.client.isConnected())
			{
				nLen = this.in.read(buffer);
			}
		}
		catch (Exception e)
		{
			AlarmMgr.getInstance().insert(2008, "telnet", "telnet", "采集问题", 1000);
			this.log.error("Telnet> readData error.", e);
		}	
		return nLen;
	}

	public boolean findFile(String strName, String end, long timeout)
	{
		boolean bIsExist = false;
		try
		{
			sendCmd("ls " + strName);

			Thread.sleep(2000L);

			byte[] buffer = new byte[10240];

			int ret = readData(buffer);

			if (ret != -1)
			{
				String strRet = new String(buffer, 0, ret);

				if ((strRet.indexOf("No such file or directory") == -1) && 
						(strRet.indexOf("not found") == -1))
				{
					bIsExist = true;
				}
			}
		}
		catch (Exception e)
		{
			this.log.error("Telnet> findFile error.", e);
		}
		return bIsExist;
	}

	public boolean waitForString(String end, long timeout)
		throws Exception
	{
		byte[] buffer = new byte[10240];

		long starttime = System.currentTimeMillis();
		try
		{
			String readbytes = new String();

			while ((readbytes.indexOf(end) < 0) && 
					(System.currentTimeMillis() - starttime < timeout * 1000L))
			{
				if (this.in.available() > 0)
				{
					int ret_read = readData(buffer);
					if (ret_read != -1) {
						readbytes = readbytes + new String(buffer, 0, ret_read);
					}
				}
				else
				{
					Thread.sleep(500L);
				}
			}
			return readbytes.indexOf(end) >= 0;
		}
		catch (Exception e)
		{
			this.log.error("Telnet> waitForString error.", e);
		}
		return false;
	}
}