package com.turk.util;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.Address;
import javax.mail.Authenticator;
import javax.mail.Message.RecipientType;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;
import org.apache.log4j.Logger;

public class Email
{
	public static final int TO = 0;
	public static final int CC = 1;
	public static final int BCC = 2;
	private String mailSMTPHost = null;

	private String mailUser = null;

	private String mailPassword = null;

	private String mailFromAddress = null;

	private String mailSubject = "";

	private Address[] mailTOAddress = null;

	private Address[] mailCCAddress = null;

	private Address[] mailBCCAddress = null;

	private MimeMultipart mailBody = null;

	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	public Email()
	{
		this.mailBody = new MimeMultipart();
	}

	public void setSMTPHost(String strSMTPHost, String strUser, String strPassword)
	{
		if ((strSMTPHost != null) && (!"".equals(strSMTPHost)))
		{
			this.mailSMTPHost = strSMTPHost;
		}
		if ((strUser != null) && (!"".equals(strUser)))
		{
			this.mailUser = strUser;
		}
		if ((strPassword != null) && (!"".equals(strPassword)))
		{
			this.mailPassword = strPassword;
		}
	}

	public void setFromAddress(String strFromAddress)
	{
		if ((strFromAddress != null) && (!strFromAddress.equals("")))
		{
			this.mailFromAddress = strFromAddress;
		}
	}

	public void setAddress(String[] strAddress, int iAddressType)
    	throws AddressException
    {
		int len = 0;
		if ((strAddress != null) && ((len = strAddress.length) > 0))
		{
			switch (iAddressType)
			{
				case 0:
					this.mailTOAddress = new Address[len];
					for (int i = 0; i < len; i++)
					{
						this.mailTOAddress[i] = new InternetAddress(strAddress[i]);
					}

					break;
				case 1:
					this.mailCCAddress = new Address[len];
					for (int i = 0; i < len; i++)
					{
						this.mailCCAddress[i] = new InternetAddress(strAddress[i]);
					}

					break;
				case 2:
					this.mailBCCAddress = new Address[len];
					int i = 0;
					while (true) 
					{
						this.mailBCCAddress[i] = new InternetAddress(strAddress[i]);

						i++; if (i >= len)
						{
							break;
						}
					}
			}
		}
 	 }

	public void setSubject(String strSubject)
	{
		if ((strSubject != null) && (!"".equals(strSubject)))
		{
			this.mailSubject = strSubject;
		}
	}

	public void setTextBody(String strTextBody)
    	throws MessagingException
	{
		if ((strTextBody != null) && (!"".equals(strTextBody)))
		{
			MimeBodyPart mimebodypart = new MimeBodyPart();
			mimebodypart.setText(strTextBody, "GBK");
			this.mailBody.addBodyPart(mimebodypart);
		}
	}

	public void setHtmlBody(String strHtmlBody)
		throws MessagingException
    {
		if ((strHtmlBody != null) && (!"".equals(strHtmlBody)))
		{
			MimeBodyPart mimebodypart = new MimeBodyPart();
			mimebodypart.setDataHandler(new DataHandler(strHtmlBody, "text/html;charset=GBK"));
			this.mailBody.addBodyPart(mimebodypart);
		}
    }

	public void setURLAttachment(String strURLAttachment)
    	throws MessagingException, MalformedURLException
    {
		if ((strURLAttachment != null) && (!"".equals(strURLAttachment)))
		{
			MimeBodyPart mimebodypart = new MimeBodyPart();
			mimebodypart.setDataHandler(new DataHandler(new URL(strURLAttachment)));
			this.mailBody.addBodyPart(mimebodypart);
		}
    }

	public void setFileAttachment(String strFileAttachment)
    	throws MessagingException, UnsupportedEncodingException
    {
		if ((strFileAttachment != null) && (!"".equals(strFileAttachment)))
		{
			File path = new File(strFileAttachment);
			if ((!path.exists()) || (path.isDirectory())) 
				return;
			String strFileName = path.getName();
			MimeBodyPart mimebodypart = new MimeBodyPart();
			mimebodypart.setDataHandler(new DataHandler(new FileDataSource(strFileAttachment)));
			mimebodypart.setFileName(MimeUtility.encodeText(strFileName));
			this.mailBody.addBodyPart(mimebodypart);
		}
    }	

	public void sendBatch()
		throws MessagingException
    {
		Properties properties = new Properties();
		properties.put("mail.smtp.host", this.mailSMTPHost);
		properties.put("mail.smtp.auth", "true");

		SmtpAuth sa = new SmtpAuth();
		sa.setUserinfo(this.mailUser, this.mailPassword);

		Session session = Session.getInstance(properties, sa);
		MimeMessage mimemessage = new MimeMessage(session);
		mimemessage.setFrom(new InternetAddress(this.mailFromAddress));
		if (this.mailTOAddress != null)
		{
			mimemessage.addRecipients(Message.RecipientType.TO, this.mailTOAddress);
		}
		if (this.mailCCAddress != null)
		{
			mimemessage.addRecipients(Message.RecipientType.CC, this.mailCCAddress);
		}
		if (this.mailBCCAddress != null)
		{
			mimemessage.addRecipients(Message.RecipientType.BCC, this.mailBCCAddress);
		}
		mimemessage.setSubject(this.mailSubject);
		mimemessage.setContent(this.mailBody);
		mimemessage.setSentDate(new Date());
   		Transport transport = session.getTransport("smtp");
   		transport.connect(this.mailSMTPHost, this.mailUser, this.mailPassword);
   		Transport.send(mimemessage);
   		this.log.debug("已向下列邮箱发送了邮件!");
   		if (this.mailTOAddress != null)
   		{
   			for (int i = 0; i < this.mailTOAddress.length; i++)
   			{
   				this.log.debug(this.mailTOAddress[i]);
   			}
   		}
   		if (this.mailCCAddress != null)
   		{
   			for (int i = 0; i < this.mailCCAddress.length; i++)
   			{
   				this.log.debug(this.mailCCAddress[i]);
   			}
   		}
   		if (this.mailBCCAddress != null)
   		{
   			for (int i = 0; i < this.mailBCCAddress.length; i++)
   			{
   				this.log.debug(this.mailBCCAddress[i]);
   			}
   		}
    }

	public static void main(String[] args)
	{
		Email mail = new Email();
		mail.setSMTPHost("smtp.exmail.qq.com", "huangx@utele.cn", "feinzaghi");
		mail.setFromAddress("huangx@utele.cn");
		mail.setSubject("测试");
		try
		{
			mail.setTextBody("这是一个测试信息哦！");
			mail.setAddress("huangx@utele.cn".split(";"), 0);
			mail.sendBatch();
		}
		catch (MessagingException e)
		{
			e.printStackTrace();
		}
	}

	static class SmtpAuth extends Authenticator
	{
		private String user;
		private String password;

		public void setUserinfo(String getuser, String getpassword)
		{
			this.user = getuser;
			this.password = getpassword;
		}
		
		protected PasswordAuthentication getPasswordAuthentication()
		{
			return new PasswordAuthentication(this.user, this.password);
		}
	}
}