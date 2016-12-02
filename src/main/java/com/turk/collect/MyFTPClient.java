package com.turk.collect;

import java.lang.reflect.Field;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;

public class MyFTPClient extends FTPClient
{
	public void configure(FTPClientConfig config)
	{
		try
		{
			Class<?> cls = getClass().getSuperclass();
			Field fi = cls.getDeclaredField("__entryParser");
			fi.setAccessible(true);
			fi.set(this, null);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		super.configure(config);
	}
}