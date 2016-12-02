package com.turk.console;

import java.io.PrintWriter;

public class ConsolePrinter
{
	private PrintWriter pw = null;
	//独立于系统的换行符
	private static String newline = System.getProperty("line.separator");
  
	public ConsolePrinter(PrintWriter pw)
	{
		this.pw = pw;
		printHello();
	}

	public void close()
	{
		this.pw.close();
	}

	public void printHello()
	{
		println();
		println("----------------------------------------------------");
		println("                Welcome to CapricornV2              ");
		println("     Copyright @ Utele All Rights Reserved          ");
		println("----------------------------------------------------");
		println();
		try
		{
			Thread.sleep(200L);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
	}

	public void printHelp()
	{
	    println("list          列出系统当前正处理的任务");
	    println("kill id       强行终止指定任务(id为任务编号或者补采编号)");
	    println("os            获取操作系统信息");
	    println("jvm           获取JVM的版本信息及JVM内存消耗情况");
	    println("ver           获取采集系统版本信息");
	    println("disk          获取磁盘信息");
	    println("stop          停止采集系统 (等待任务执行完后在停止)");
	    println("stop -i       立即停止采集系统");
	    println("date          获取服务器当前时间");
	    println("host          获取服务器机器名");
	    println("sys           获取系统信息");
	    println("error         获取采集系统标准错误端信息");
	    println("thread -c     获取采集系统内部线程个数");
	    println("whoami        获取当前用户信息");
	    println("exit          退出会话");
	    println("help/?        获取帮助");
	}

	public void printUnSupportCmd(String cmd)
	{
	    println("不支持的命令 " + cmd);
	    println("输入help或者?获取帮助");
	}

	public void println()
	{
	    this.pw.println("");
	    this.pw.flush();
	}

	public void println(String content)
	{
	    this.pw.print("" + content + newline);
	    this.pw.flush();
	}

	public void print(String content)
	{
	    this.pw.print("" + content);
	    this.pw.flush();
	}	

	public void backspace()
	{
	    this.pw.print(' ');
	    this.pw.print('\b');
	    this.pw.flush();
	}

	public void maskChar()
	{
	    this.pw.print('\b');
	    this.pw.print(' ');
	    this.pw.flush();
	}

	public void printNull()
	{
	    this.pw.print(' ');
	    this.pw.flush();
	}

	public void printPrompt()
  	{
		print("> ");
  	}
}