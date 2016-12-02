package com.turk.console;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.turk.db.dao.UserDAO;
import com.turk.db.pojo.User;
import com.turk.util.Util;

public class RequestHandler
  implements Runnable
{
	private Socket socket;
	private ConsolePrinter printer = null;
	private CmdHandler cmdHandler = null;
	private String userName;
	private String userPwd;
	private Date loginTime;
	private boolean loginFlag = false;
	private int loginTimes = 0;
	public static final List<Socket> SOCKETS = new ArrayList<Socket>();

	public RequestHandler(Socket soeckt)
	{
		this.socket = soeckt;
		if (soeckt != null)
		{
			synchronized (SOCKETS)
			{
				SOCKETS.add(soeckt);
			}
		}
	}

	public void run()
	{
		try
		{
			this.printer = new ConsolePrinter(new PrintWriter(this.socket.getOutputStream()));
			this.cmdHandler = new CmdHandler(this.printer);
			InputStream in = this.socket.getInputStream();

			handleLogin(in);
			if (!this.loginFlag) {
				return;
			}
			String strLine = this.cmdHandler.getInput(in);
			do
			{
				handleCommand(strLine, in);
				this.printer.printPrompt();
				strLine = this.cmdHandler.getInput(in);

				if (strLine == null) break; 
			}while (!strLine.trim().equalsIgnoreCase("exit"));
		}
		catch (SocketException localSocketException)
    	{
			if (this.printer != null) {
				this.printer.close();
			}
			if (this.socket != null)
			{
				try
				{
					this.socket.close();
					synchronized (SOCKETS)
					{
						SOCKETS.remove(this.socket);
					}
				}
				catch (IOException localIOException2)
				{
				}
			}
    	}
		catch (IOException e)
		{
			e.printStackTrace();

			if (this.printer != null) {
				this.printer.close();
			}
			if (this.socket != null)
			{
				try
				{
					this.socket.close();
					synchronized (SOCKETS)
					{
						SOCKETS.remove(this.socket);
					}
				}
				catch (IOException localIOException3)
				{
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();

			if (this.printer != null) {
				this.printer.close();
			}
			if (this.socket != null)
			{
				try
				{
					this.socket.close();
					synchronized (SOCKETS)
					{
						SOCKETS.remove(this.socket);
					}
				}
				catch (IOException localIOException4)
				{
				}
			}
		}
		finally
		{
			if (this.printer != null) {
				this.printer.close();
			}
			if (this.socket != null)
			{
				try
				{
					this.socket.close();
					synchronized (SOCKETS)
					{
						SOCKETS.remove(this.socket);
					}
				}
		        catch (IOException localIOException5)
		        {
		        }
			}
		}
	}

	private void handleLogin(InputStream in)
    	throws Exception
    {
		this.printer.print("Login: ");

		String strLine = this.cmdHandler.getInput(in);
		while (strLine != null)
		{
			this.userName = strLine.trim();

			if (this.userName.length() == 0)
			{
				this.printer.print("Login: ");
				strLine = this.cmdHandler.getInput(in);
			}
			else
			{
				this.printer.print("Password: ");
				strLine = this.cmdHandler.getInput(in, false);
				if (strLine == null)
				{
					this.printer.print("Login: ");
					strLine = this.cmdHandler.getInput(in);
				}
				else
				{
					this.userPwd = strLine.trim();

					User u = new User();
					u.setUserName(this.userName);
					u.setUserPwd(this.userPwd);
					this.loginFlag = new UserDAO().checkAccount(u);
					if (this.loginFlag)
					{
						this.loginTime = new Date();
						String hostName = Util.getHostName();
						Properties props = System.getProperties();
						String osName = props.getProperty("os.name");
						String osVersion = props.getProperty("os.version");

						this.printer.println("Hello " + this.userName + ".   " + osName + " " + 
								osVersion + "   " + hostName + "   " + 
								Util.getDateString(this.loginTime));
						this.printer.printPrompt();
						break;
					}

					this.loginTimes += 1;
					if (this.loginTimes >= 3)
					{
						break;
					}
					this.printer.println("Login incorrect");
					this.printer.print("Login: ");
					strLine = this.cmdHandler.getInput(in);
				}
			}
		}
    }

	private void handleCommand(String cmd, InputStream in)
	{
		if (!this.loginFlag) {
			return;
		}
		if (Util.isNull(cmd)) {
			return;
		}
		if (cmd.equalsIgnoreCase("list"))
		{	
			this.cmdHandler.list();
		}
		else if (cmd.startsWith("kill"))
		{
			this.cmdHandler.kill(cmd, in);
		}
		else if (cmd.equalsIgnoreCase("os"))
		{
			this.cmdHandler.os();
		}
		else if (cmd.equalsIgnoreCase("jvm"))
		{
			this.cmdHandler.jvm();
		}
		else if (cmd.equalsIgnoreCase("ver"))
		{
			this.cmdHandler.ver();
		}
		else if (cmd.equalsIgnoreCase("disk"))
		{
			this.cmdHandler.disk();
		}
		else if (cmd.equalsIgnoreCase("date"))
		{
			this.cmdHandler.date();
		}
		else if (cmd.equalsIgnoreCase("host"))
		{
			this.cmdHandler.host();
		}
		else if (cmd.equalsIgnoreCase("sys"))
		{
			this.cmdHandler.sys();
		}
		else if (cmd.equalsIgnoreCase("error"))
		{
			this.cmdHandler.error();
		}
		else if (cmd.equalsIgnoreCase("whoami"))
		{
			this.printer.println(this.userName + "    login time: " + 
					Util.getDateString(this.loginTime));
		}
		else if (cmd.startsWith("thread"))
		{
			this.cmdHandler.thread(cmd);
		}
		else if (cmd.startsWith("stop"))
		{
			this.cmdHandler.stop(cmd, in);
		}
		else if ((cmd.equalsIgnoreCase("help")) || (cmd.equalsIgnoreCase("?")))
		{
			this.printer.printHelp();
		}
		else
		{
			this.printer.printUnSupportCmd(cmd);
		}
	}
}