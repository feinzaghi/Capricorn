package com.turk.console;

import java.util.Date;
import java.util.Properties;

import com.turk.console.common.console.command.CommandAction;
import com.turk.console.common.console.io.CommandIO;
import com.turk.db.dao.UserDAO;
import com.turk.db.pojo.User;
import com.turk.util.Util;

/**
 * Telnet ÓÃ»§µÇÂ¼
 * @author Administrator
 *
 */
public class LoginAction
  	implements CommandAction
{
	public boolean handleCommand(String[] args, CommandIO io)
    	throws Exception
    {
		io.setPrefix(">");
		io.setLeftPadding("");
		boolean loginFlag = false;
		int count = 0;
		while ((!loginFlag) && (count++ < 3))
		{
			String u = io.readLine("login:");
			String p = io.readLine("password:", false);
			User user = new User();
			user.setUserName(u);
			user.setUserPwd(p);
			loginFlag = new UserDAO().checkAccount(user);
			if (loginFlag)
			{
				Date loginTime = new Date();
				String hostName = Util.getHostName();
				Properties props = System.getProperties();
				String osName = props.getProperty("os.name");
				String osVersion = props.getProperty("os.version");

				io.println("Hello " + u + ".   " + osName + " " + osVersion + 
						"   " + hostName + "   " + 
						Util.getDateString(loginTime));
				return true;
			}

			io.println("µÇÂ¼Ê§°Ü");
		}
		
		return false;
    }
}