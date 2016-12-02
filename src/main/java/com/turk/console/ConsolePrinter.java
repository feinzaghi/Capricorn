package com.turk.console;

import java.io.PrintWriter;

public class ConsolePrinter
{
	private PrintWriter pw = null;
	//������ϵͳ�Ļ��з�
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
	    println("list          �г�ϵͳ��ǰ�����������");
	    println("kill id       ǿ����ָֹ������(idΪ�����Ż��߲��ɱ��)");
	    println("os            ��ȡ����ϵͳ��Ϣ");
	    println("jvm           ��ȡJVM�İ汾��Ϣ��JVM�ڴ��������");
	    println("ver           ��ȡ�ɼ�ϵͳ�汾��Ϣ");
	    println("disk          ��ȡ������Ϣ");
	    println("stop          ֹͣ�ɼ�ϵͳ (�ȴ�����ִ�������ֹͣ)");
	    println("stop -i       ����ֹͣ�ɼ�ϵͳ");
	    println("date          ��ȡ��������ǰʱ��");
	    println("host          ��ȡ������������");
	    println("sys           ��ȡϵͳ��Ϣ");
	    println("error         ��ȡ�ɼ�ϵͳ��׼�������Ϣ");
	    println("thread -c     ��ȡ�ɼ�ϵͳ�ڲ��̸߳���");
	    println("whoami        ��ȡ��ǰ�û���Ϣ");
	    println("exit          �˳��Ự");
	    println("help/?        ��ȡ����");
	}

	public void printUnSupportCmd(String cmd)
	{
	    println("��֧�ֵ����� " + cmd);
	    println("����help����?��ȡ����");
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