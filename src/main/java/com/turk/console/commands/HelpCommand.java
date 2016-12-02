package com.turk.console.commands;

import com.turk.console.common.console.io.CommandIO;

public class HelpCommand extends BasicCommand
{
	private static String HELP_INFO = "list          �г�ϵͳ��ǰ�����������\r\n   os            ��ȡ����ϵͳ��Ϣ\r\n   jvm           ��ȡJVM�İ汾��Ϣ��JVM�ڴ��������\r\n   ver           ��ȡ�ɼ�ϵͳ�汾��Ϣ\r\n   disk          ��ȡ������Ϣ\r\n   stop          ֹͣ�ɼ�ϵͳ (�ȴ�����ִ�������ֹͣ)\r\n   stop -i       ����ֹͣ�ɼ�ϵͳ\r\n   date          ��ȡ��������ǰʱ��\r\n   host          ��ȡ������������\r\n   sys           ��ȡϵͳ��Ϣ\r\n   error         ��ȡ�ɼ�ϵͳ��׼�������Ϣ\r\n   thread -c     ��ȡ�ɼ�ϵͳ�̳߳���Ϣ\r\n   exit          �˳��Ự\r\n   help/?        ��ȡ����";

	public boolean doCommand(String[] args, CommandIO io)
		throws Exception
    {
		io.println(HELP_INFO);
		return true;
    }
}