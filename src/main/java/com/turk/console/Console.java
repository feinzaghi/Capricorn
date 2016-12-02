package com.turk.console;

import com.turk.config.SystemConfig;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

/**
 * 控制台
 * @author Administrator
 *
 */
public class Console
{
	private ServerSocket serverSocket;
	private static Console instance = null;
	private boolean runFlag = true;
	private static final Logger logger = Logger.getLogger(Console.class);
	private RequestAccept accept;
	private ExecutorService executor;
	private int maxClientCount;

	public static synchronized Console getInstance()
	{
		if (instance == null) {
			instance = new Console();
		}
		return instance;
	}

	/**
	 * 启动控制台监听
	 * @throws IOException
	 */
	public void start() throws IOException
	{
		if (this.serverSocket == null)
		{
			int port = SystemConfig.getInstance().getCollectPort();
			this.serverSocket = new ServerSocket(port);
			listen();
		}
	}

	private synchronized boolean isRun()
	{
		return this.runFlag;
	}

	private void listen()
	{
		if (this.serverSocket == null)
			return;
		try
		{
			logger.info("开始侦听控制台命令，请使用telnet登录到:" + InetAddress.getByName(null).getHostAddress() + " " + 
					this.serverSocket.getLocalPort());
		}
		catch (UnknownHostException localUnknownHostException)
		{
		}
		int maxClientCount = Runtime.getRuntime().availableProcessors() + 1;
		this.executor = Executors.newFixedThreadPool(maxClientCount);

		this.accept = new RequestAccept();
		this.accept.start();
	}
	
	public void stop()
	{
		if (this.executor != null)
		{
			this.executor.shutdown();
			for (Socket s : RequestHandler.SOCKETS)
			{
				if (s == null)
					continue;
				try
				{
					s.close();
				}
				catch (Exception localException)
				{
				}
			}

			this.executor.shutdownNow();
		}
	}

	public static void main(String[] args)
	{
		try
		{
			getInstance().start();
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	public int getMaxClientCount()
	{
		return this.maxClientCount;
	}

	class RequestAccept extends Thread
	{
		RequestAccept()
		{
		}

		public void run()
		{
			while (Console.this.isRun())
			{
				try
				{
					Socket s = Console.this.serverSocket.accept();
					RequestHandler reqHandler = new RequestHandler(s);
					Console.this.executor.execute(reqHandler);
				}
				catch (IOException e)
				{
					Console.logger.error("控制台异常,原因:", e);
				}
			}
		}
	}
}