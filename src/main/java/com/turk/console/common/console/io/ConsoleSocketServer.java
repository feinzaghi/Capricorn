package com.turk.console.common.console.io;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.turk.console.common.console.command.Command;
import com.turk.console.common.console.command.CommandAction;
import com.turk.console.common.console.session.Session;
import com.turk.console.common.console.session.SessionContext;
import com.turk.console.common.console.util.Util;

/**
 * 控制台服务
 * @author Administrator
 *
 */
public class ConsoleSocketServer
{
	private ServerSocket serverSocket;
	private CommandAction cmdNotFoundAction;
	private CommandAction loginAction;
	private Map<String, Command> commands;
	private String welcome;
	private Thread mainThread;
	protected int port;
	private boolean stopFlag = false;
	
	private List<Session> sessions = new ArrayList<Session>();

	public ConsoleSocketServer(int port, Map<String, Command> commands, CommandAction cmdNotFoundAction, CommandAction loginAction, String welcome)
		throws IOException
    {
		this.port = port;
		this.commands = commands;
		this.cmdNotFoundAction = cmdNotFoundAction;
		this.loginAction = loginAction;
		this.welcome = welcome;
		this.serverSocket = new ServerSocket(this.port);
		Util.debug(this.serverSocket);
    }

	/*
	 * 启动服务
	 */
 	public void start()
 		throws IOException
    {
 		this.mainThread = new Thread(new Runnable()
 		{
 			public void run()
 			{
 				while (!ConsoleSocketServer.this.isStop())
 				{
 					try
 					{
 						Socket clientSocket = ConsoleSocketServer.this.serverSocket.accept();
 						Session session = new Session(new SessionContext(clientSocket, ConsoleSocketServer.this.serverSocket), ConsoleSocketServer.this.commands, 
 								ConsoleSocketServer.this.cmdNotFoundAction, ConsoleSocketServer.this.loginAction, ConsoleSocketServer.this.welcome);
 						
 						session.start();
 						
 						ConsoleSocketServer.this.sessions.add(session);
 					
 					}
 					catch (Exception e)
 					{
 						e.printStackTrace();
 					}
 				}
 			}
 		});
 		this.mainThread.start();
    }
 	
 	public List<Session> getSessions()
 	{	
 		return this.sessions;
 	}

 	public synchronized void stop()
 	{
 		this.stopFlag = true;
 	}
 	
 	private synchronized boolean isStop()
 	{
 		return this.stopFlag;
 	}
}
