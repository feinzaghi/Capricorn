package com.turk.alarm;

import com.turk.Config.SystemConfig;
import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import org.apache.log4j.Logger;
import com.turk.util.Email;
import com.turk.util.LogMgr;

public class EMailAlarmSender
  	implements AlarmSender
{
	private Logger log = LogMgr.getInstance().getSystemLogger();

	private String content = "告警描述：\t\t任务号:%s\t\t错误源:%s\t\t错误描述:%s\t\t发生时间:%s\t\t告警级别:%s\t\t错误码:%s";

	public byte send(Alarm alarm)
	{
		byte result = -1;
		if (alarm != null)
		{
			Email mail = new Email();
			SystemConfig config = SystemConfig.getInstance();
			if (config == null) 
				return result;

			try
			{
				String[] to = config.getMailTO();

				String[] cc = config.getMailCC();

				String[] bcc = config.getMailBCC();

				String host = config.getMailSMTPHost();

				String account = config.getMailAccount();

				String password = config.getMailPassword();

				String subjcet = alarm.getTitle();

				String newCnt = String.format(this.content, new Object[] { Integer.valueOf(alarm.getTaskID()), alarm.getSource(), alarm.getDescription(), alarm.getOccuredTime(), Byte.valueOf(alarm.getAlarmLevel()), Integer.valueOf(alarm.getErrorCode()) });

				mail.setAddress(to, 0);
				mail.setAddress(cc, 1);
				mail.setAddress(bcc, 2);
				mail.setSMTPHost(host, account, password);
				mail.setFromAddress(account);
				mail.setSubject(subjcet);
				mail.setHtmlBody(newCnt);
				mail.sendBatch();
				result = 0;
			}
			catch (AddressException e)
			{
				this.log.error("邮件地址异常！", e);
				result = -1;
			}
			catch (MessagingException e)
			{
				this.log.error("邮件异常！", e);
				result = -1;
			}
		}

		return result;
	}

	public static void main(String[] args)
	{
		EMailAlarmSender e = new EMailAlarmSender();
		e.send(new Alarm());
	}
}