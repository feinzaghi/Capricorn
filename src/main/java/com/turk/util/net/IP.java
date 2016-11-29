package com.turk.util.net;

public class IP
{
	/**
	 * IP 地址格式验证
	 * @param ipAddr
	 * @return
	 */
	public static boolean isValidIpAddress(String ipAddr)
	{
		if ((ipAddr == null) || (ipAddr.length() < 7)) {
			return false;
		}
		String[] parts = ipAddr.split("[.]");
		
		if (parts.length != 4) 
			return false;

		for (int i = 0; i < parts.length; i++)
		{
			int ipart = -1;
			try
			{
				ipart = Integer.parseInt(parts[i]);
			}
			catch (NumberFormatException e)
			{
				return false;
			}
			if ((i == 0) && (ipart <= 0))
				return false;
			if ((ipart < 0) || (ipart > 255)) 
				return false;
		}

		return true;
	}

	public static long ip2Long(String strIp)
	{
		long[] ip = new long[4];

	    int position1 = strIp.indexOf(".");
	    int position2 = strIp.indexOf(".", position1 + 1);
	    int position3 = strIp.indexOf(".", position2 + 1);
	
	    ip[0] = Long.parseLong(strIp.substring(0, position1));
	    ip[1] = Long.parseLong(strIp.substring(position1 + 1, position2));
	    ip[2] = Long.parseLong(strIp.substring(position2 + 1, position3));
	    ip[3] = Long.parseLong(strIp.substring(position3 + 1));

	    return (ip[0] << 24) + (ip[1] << 16) + (ip[2] << 8) + ip[3];
	}

	public static String long2IP(long longIp)
	{
	    StringBuffer sb = new StringBuffer("");
	
	    sb.append(String.valueOf(longIp >>> 24));
	    sb.append(".");
	
	    sb.append(String.valueOf((longIp & 0xFFFFFF) >>> 16));
	    sb.append(".");
	
	    sb.append(String.valueOf((longIp & 0xFFFF) >>> 8));
	    sb.append(".");
	
	    sb.append(String.valueOf(longIp & 0xFF));
	    return sb.toString();
	}

	public static void main(String[] args)
	{
		System.out.println(isValidIpAddress("127.x.0.1"));
		System.out.println(ip2Long("192.168.0.1"));
		System.out.println(long2IP(3232235521L));
	}
}