package com.turk.console.common.console.session;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

public class SessionContext
{
	private Socket clientSocket;
	private ServerSocket serverSocket;
	private Date beginTime;
	private Date endTime;

	public SessionContext(Socket clientSocket, ServerSocket serverSocket)
	{	
		this.clientSocket = clientSocket;
		this.serverSocket = serverSocket;
	}

	public Socket getClientSocket()
	{
		return this.clientSocket;
	}

	public ServerSocket getServerSocket()
	{
		return this.serverSocket;
	}

	public Date getBeginTime()
	{
		return this.beginTime;
	}

	public void setBeginTime(Date beginTime)
	{
		this.beginTime = beginTime;
	}	

	public Date getEndTime()
	{
		return this.endTime;
	}

	public void setEndTime(Date endTime)
	{
		this.endTime = endTime;
	}

	public long getCostTime()
	{
		if (this.beginTime == null) return 0L;
		return System.currentTimeMillis() - this.beginTime.getTime();
	}

	public boolean isRunning()
	{
		return this.endTime == null;
	}
}
