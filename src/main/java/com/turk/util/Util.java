package com.turk.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.CRC32;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPClientConfig;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
//import task.CollectObjInfo;

import org.nfunk.jep.JEP;

import com.turk.Config.SystemConfig;

public class Util
{
	private static String gpsRegEx = ".*_((\\.*[a-zA-Z]{2,30}))_.*";
	private static Logger log = LogMgr.getInstance().getSystemLogger();

	public static String getHostName()
  	{
		String strHostName = null;
		try
		{
			strHostName = InetAddress.getLocalHost().getHostName();
		}
		catch (UnknownHostException localUnknownHostException)
		{
		}

		return strHostName;
  	}

	public static void showOSState()
	{
		long maxMemory = Runtime.getRuntime().maxMemory() / 1024L;
		long totalMemory = Runtime.getRuntime().totalMemory() / 1024L;
		long freeMemory = Runtime.getRuntime().freeMemory() / 1024L;
		long usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;

		log.debug("OS State: mem used:" + usedMemory + "KB,mem free:" + 
				freeMemory + "KB,mem total:" + totalMemory + "KB.");
	}

	public static void printEnvironmentInfo()
	{
		try
		{
			Properties props = System.getProperties();

			StringBuffer sb = new StringBuffer();

			sb.append("\n----------------------Environment Info--------------------------\n");
			sb.append("os.name : " + props.getProperty("os.name") + "\n");
			sb.append("os.arch : " + props.getProperty("os.arch") + "\n");
			sb.append("os.version : " + props.getProperty("os.version") + "\n");

			sb.append("java.version : " + props.getProperty("java.version") + "\n");
			sb.append("java.vendor : " + props.getProperty("java.vendor") + "\n");
			sb.append("java.vm.name : " + props.getProperty("java.vm.name") + "\n");
			sb.append("java.home : " + props.getProperty("java.home") + "\n");

			sb.append("java.class.path : " + props.getProperty("java.class.path") + "\n");
			sb.append("java.library.path : " + props.getProperty("java.library.path") + "\n");

			sb.append("user.name : " + props.getProperty("user.name") + "\n");
			sb.append("user.home : " + props.getProperty("user.home") + "\n");
			sb.append("user.dir : " + props.getProperty("user.dir") + "\n");

			sb.append("file.encoding : " + props.getProperty("file.encoding") + "\n");

			sb.append("--------Disk information---------\n");
			printDiskInfo(sb);

			sb.append("----------------------------------------------------------------");

			log.info(sb.toString());
		}
		catch (Exception localException)
		{
		}
	}

	public static void printDiskInfo(StringBuffer sb)
	{
		try
		{
			File[] roots = File.listRoots();
			for (File _file : roots)
			{
				sb.append(_file.getPath() + "\n");
				sb.append("Free space = " + _file.getFreeSpace() + "\n");
				sb.append("Usable space = " + _file.getUsableSpace() + "\n");
				sb.append("Total space = " + _file.getTotalSpace() + "\n");
				sb.append("\n");
			}
		}
		catch (Exception localException)
		{
		}
	}

	public static void printMemoryStatus()
  	{
		float maxMemory = (float)(Runtime.getRuntime().maxMemory() / 1048576L);
		float totalMemory = (float)(Runtime.getRuntime().totalMemory() / 1048576L);
		float freeMemory = (float)(Runtime.getRuntime().freeMemory() / 1048576L);
		float usedMemory = totalMemory - freeMemory;
		freeMemory = maxMemory - usedMemory;
  		System.out.println("已使用: " + usedMemory + "M  剩余: " + freeMemory + "M  最大内存: " + maxMemory + "M");
  	}
	
	
	/**
	 * 整形转换为IP
	 * @param ipInt
	 * @return
	 */
	public static String int2ip(long ipInt){
		
		StringBuilder sb=new StringBuilder();
		sb.append((ipInt>>24)&0xFF).append(".");
		sb.append((ipInt>>16)&0xFF).append(".");
		sb.append((ipInt>>8)&0xFF).append(".");
		sb.append(ipInt&0xFF);
		return sb.toString(); 
		
	} 


	public static String string2Hex(String b)
	{
		if ((b == null) || (b.equals(""))) {
			return null;
		}
		return bytes2Hex(b.getBytes());
	}

	public static String bytes2Hex(byte[] b)
	{
		if (b == null)
			return null;
		String ret = "";
		for (int i = 0; i < b.length; i++)
		{
			String hex = Integer.toHexString(b[i] & 0xFF);
			if (hex.length() == 1)
			{
				hex = '0' + hex;
			}
			ret = ret + hex.toUpperCase();
		}
		return ret;
	}
  
	public static byte[] objectToBytes(Object obj) throws Exception
	{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		ObjectOutputStream sOut = new ObjectOutputStream(out);
		sOut.writeObject(obj);
		sOut.flush();
		byte[] bytes = out.toByteArray();
		return bytes;
	}

	public static int execExternalCmd(String cmd)
    	throws Exception
    {
		ExternalCmd externalCmd = new ExternalCmd();
		return externalCmd.execute(cmd);
    }

	public static boolean isNull(String str)
	{
		boolean bReturn = false;
		if ((str == null) || (str.trim().equals("")))
		{
			bReturn = true;
		}
		return bReturn;
	}

	public static boolean isNotNull(String str)
	{
		boolean bReturn = false;
		if ((str != null) && (!str.trim().equals("")))
		{
			bReturn = true;
		}
		return bReturn;
	}

	public static char[] bytesToChars(byte[] bytes)
	{
		String s = new String(bytes);
		char[] c = s.toCharArray();
		return c;
	}

	public static int parseExpression(String str)
	{
		JEP myParser = new JEP();
		myParser.parseExpression(str);
		return (int)myParser.getValue();
	}

	public static Object strSerialization(String str,Class c)
	{
		Object obj=null;
		try{
			JAXBContext context=JAXBContext.newInstance(c);
			Unmarshaller unmarshaller = context.createUnmarshaller(); 
			 
			obj= unmarshaller.unmarshal(new StringReader(str));
		}
		catch(Exception ex){
		}
		return obj;
	}
	
 	public static String trimHour(int hour)
 	{
 		String str = String.valueOf(hour);
 		if (str.length() == 1)
 		{
 			str = "0" + str;
 		}
 		return str;
 	}

 	public static boolean isZipFile(String strFileName)
 	{
 		boolean bReturn = false;

 		if ((strFileName == null) || (strFileName.equals(""))) {
 			return false;
 		}
 		int nExIndex = strFileName.lastIndexOf(".");

 		String strExName = "";
 		if (nExIndex > 0) {
 			strExName = strFileName.substring(nExIndex).toLowerCase();
 		}
 		if ((strExName.equals(".zip")) || (strExName.equals(".tar")) || 
 				(strExName.equals(".tar.z")) || (strExName.equals(".gz")) || 
 				(strExName.equals(".tar.bz2")) || (strExName.equals(".rar")))
 		{
 			bReturn = true;
 		}
 		return bReturn;
 	}

 	public static String[] list2Array(List<String> list)
 	{
 		if (list == null)
 			return null;
 		int size = list.size();
 		String[] values = new String[size];

 		for (int i = 0; i < size; i++)
 		{
 			values[i] = ((String)list.get(i));
 		}
	    return values.length > 0 ? values : null;
 	}

 	public static String list2String(List<String> list, String split)
 	{
 		if (list == null)
 			return null;
 		StringBuilder sb = new StringBuilder();
 		for (String s : list)
 		{
 			sb.append(s).append(split);
 		}
 		return sb.toString();
 	}

 	public static String array2String(String[] strs)
 	{
 		if (strs == null)
 			return null;
 		StringBuilder sb = new StringBuilder();
 		String[] arrayOfString = strs; int j = strs.length;
 		for (int i = 0; i < j; i++) 
 		{ 
 			String s = arrayOfString[i];
 			sb.append(s).append(";");
 		}
 		return sb.toString();
 	}

 	public static boolean isWindows()
 	{
 		boolean bReturn = false;

 		String os = System.getProperties().getProperty("os.name").toLowerCase();
 		if ((os == null) && ("".equals(os))) {
 			return true;
 		}
 		if (os.indexOf("window") >= 0)
 		{
 			bReturn = true;
 		}

 		return bReturn;
 	}

 	public static boolean isOracle()
 	{
 		String strDriver = SystemConfig.getInstance().getDbDriver();
 		return strDriver.contains("oracle");
 	}

 	public static boolean isSybase()
 	{
 		String strDriver = SystemConfig.getInstance().getDbDriver();
 		return strDriver.contains("sybase");
 	}

 	public static boolean isSqlServer()
 	{
 		String strDriver = SystemConfig.getInstance().getDbDriver();
 		return strDriver.contains("sqlserver");
 	}
  
 	public static boolean isMySQL()
 	{
 		String strDriver = SystemConfig.getInstance().getDbDriver();
 		return strDriver.contains("mysql");
 	}

 	public static String getDateString(Date date)
 	{
 		String pattern = "yyyy-MM-dd HH:mm:ss";
 		SimpleDateFormat f = new SimpleDateFormat(pattern);
 		return f.format(date);
 	}
  
 	public static String getDateString(Timestamp tsp)
 	{
 		Date date = new Date(tsp.getTime());
 		return getDateString(date);
 	}
 	
 	public static String getDateString_yyyyMMddHHmmss(Date date)
 	{
 		SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmss");
 		String strTime = f.format(date);
 		return strTime;
 	}

 	public static String getDateString_yyyyMMddHH(Date date)
 	{
 		SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHH");
 		String strTime = f.format(date);
 		return strTime;
 	}

 	public static String getDateString_yyyyMMdd(Date date)
 	{
 		SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
 		String strTime = f.format(date);
 		return strTime;
 	}

 	public static String getDateString_yyyyMMddHHmmssSSS(Date date)
 	{
 		SimpleDateFormat f = new SimpleDateFormat("yyyyMMddHHmmssSSS");
 		String strTime = f.format(date);
 		return strTime;
 	}
 	
 	public static String getDateString_Standard_ss(Date date)
 	{
 		SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
 		String strTime = f.format(date);
 		return strTime;
 	}

 	public static String getDateString_yyyyMMddHHmmss(Timestamp tsp)
 	{
 		Date date = new Date(tsp.getTime());
 		return getDateString_yyyyMMddHHmmss(date);
 	}

 	public static Date getDate(String str, String pattern)
 		throws ParseException
    {
 		SimpleDateFormat f = new SimpleDateFormat(pattern);
 		Date d = f.parse(str);
 		return d;
    }

 	public static Date getDate1(String str)
 		throws ParseException
    {
 		String pattern = "yyyy-MM-dd HH:mm:ss";
 		return getDate(str, pattern);
    }

 	public static Date getDate2(String str)
 		throws ParseException
 	{
 		String pattern = "yyyyMMddhhmmss";
 		return getDate(str, pattern);
 	}

 	/**
 	 * yyyyMMdd
 	 * @param str
 	 * @return 
 	 * @throws ParseException
 	 */
 	public static Date getDate3(String str)
 		throws ParseException
	{
	 	String pattern = "yyyyMMdd";
	 	return getDate(str, pattern);
	}
  
 	public static String checkEnsurePos(String strSubFilePath)
 	{
 		if (findByRegex(strSubFilePath, gpsRegEx, 0) != null) return strSubFilePath;
 		return strSubFilePath;
 	}

 	public static String findByRegex(String str, String regEx, int group)
 	{
 		String resultValue = null;
 		if ((str == null) || (regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) 
 			return resultValue;
 		
 		
 		Pattern p = Pattern.compile(regEx);
 		Matcher m = p.matcher(str);

 		boolean result = m.matches();
 		if (result)
 		{
 			resultValue = m.group(group);
 		}
 		return resultValue;
 	}

 	public static long crc32(String str)
 	{
 		CRC32 x = new CRC32();
 		x.update(str.getBytes());
 		return x.getValue();
 	}

 	public static <T> Document pojosToXML(List<T> pojos)
 	{
 		Document doc = DocumentHelper.createDocument();
 		Element root = doc.addElement("pojos");
 		for (Iterator localIterator = pojos.iterator(); localIterator.hasNext(); ) 
 		{ 
 			Object pojo = localIterator.next();
 			Element child = root.addElement("pojo");
 			Class cls = pojo.getClass();
 			Field[] fields = cls.getDeclaredFields();

 			String DATE1 = "java.sql.Timestamp";
 			String DATE2 = "java.util.Date";

 			label220: 
 			for (Field f : fields)
 			{
 				f.setAccessible(true);
 				String name = f.getName();
 				String value = null;
 				String type = f.getType().getName();
 				if ((type.equals("java.sql.Timestamp")) || (type.equals("java.util.Date")))
 				{
 					try
 					{
 						Object obj = f.get(pojo);
 						if (obj == null)
 						{
 							value = ""; 
 							break label220;
 						}

 						Date d = (Date)obj;
 						value = getDateString(d);
 					}	
 					catch (Exception e)
 					{
 						e.printStackTrace();
 					}

 				}
 				else
 				{
 					try
 					{
 						Object obj = f.get(pojo);
 						if (obj == null)
 						{
 							value = "";
 						}
 						else
 						{
 							value = obj.toString();
 						}
 					}
 					catch (Exception e)
 					{
 						e.printStackTrace();
 					}
 				}
 				Element e = child.addElement(name);
 				e.setText(value);
 			}
 		}
 		return doc;
 	}

 	public static Document pojoToXML(Object pojo)
 	{
 		List list = new ArrayList();
 		list.add(pojo);
 		return pojosToXML(list);
 	}

 	public static String toMD5(String s)
 	{
 		if (s == null) 
 			return null;
 		StringBuilder result = new StringBuilder();

 		byte[] bs = (byte[])null;
 		try
 		{
 			bs = MessageDigest.getInstance("md5").digest(s.getBytes());
 		}
 		catch (NoSuchAlgorithmException e)
 		{
 			log.error("转md5时异常", e);
 			return null;
 		}

 		String tmp = null;
 		for (int i = 0; i < bs.length; i++)
 		{
 			tmp = Integer.toHexString(bs[i] & 0xFF);
 			if (tmp.length() == 1)
 			{
 				result.append("0");
 			}
 			result.append(tmp);
 		}

 		return result.toString();
 	}

  	public static void main(String[] args)
  		throws Exception
    {
  		//String str = findByRegex("", "[0-9]*", 0);
  		//System.out.print(str);
  		
  		String str = Util.int2ip(167773121);
  		String[] numArray = str.split("\\.",-1);
		String cellname = "";
		long lNeSysID = -10000;
		if(numArray.length==4)
		{
			int LAC= Integer.parseInt(numArray[1]) * 256 + Integer.parseInt(numArray[0]);
			int CI = Integer.parseInt(numArray[3]) * 256 + Integer.parseInt(numArray[2]);
			cellname = LAC+"_"+CI;
			
		}
		log.debug(cellname);
		LogMgr.getInstance().getAppLogger("error").error(cellname);
		LogMgr.getInstance().getAppLogger("taurus").error(cellname);
  		//System.out.print(cellname);
    }
  	
  	private static void listFile(File file)
  	{
  		for(File f : file.listFiles())
		{
  			if(f.isDirectory())
  			{
  				listFile(f);
  				//f.renameTo(new File("D:\\temp\\data3\\"+f.getName()));
  		  		//File newfile = new File("D:\\temp\\data3\\"+f.getName());
  		  		boolean bl = f.delete();
  		  		System.out.println(bl+":"+f.getPath());
  			}
  			else
  			{
  				break;
  			}
		}
  		
  		
  		
  	}

  	public static boolean isFileNotNull(FTPFile[] fs)
  	{
  		if (fs == null) 
  			return false;
  		if (fs.length == 0) 
  			return false;
  		FTPFile[] arrayOfFTPFile = fs; 
  		int j = fs.length; 
  		for (int i = 0; i < j; i++) 
  		{ 
  			FTPFile f = arrayOfFTPFile[i];

  			if ((f == null) || (isNull(f.getName())) /*|| 
  				(f.getName().contains(" ")) */|| (f.getName().contains("\t"))) 
  				return false;
  		}
  		return true;
  	}

  	private static FTPClient newFTP(String ip, int port, String user, String pwd, String type) throws Exception
  	{
  		FTPClient ftp = new FTPClient();
  		ftp.connect(ip, port);
  		ftp.login(user, pwd);
  		ftp.configure(new FTPClientConfig(type));
  		return ftp;
  	}

  	public static FTPClient setFTPClientConfig(FTPClient ftp, String ip, int port, String user, String pwd) throws Exception
  	{
  		
  		ftp.configure(new FTPClientConfig(FTPClientConfig.SYST_NT));
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_UNIX);
  		}
  		else
  		{
  			return ftp;
  		}
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_AS400);
  		}
  		else
  		{
  			return ftp;
  		}
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_L8);
  		}
  		else
  		{
  			return ftp;
  		}
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_MVS);
  		}
  		else
  		{
  			return ftp;
  		}
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_NETWARE);
  		}
  		else
  		{
  			return ftp;
  		}
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_OS2);
  		}
  		else
  		{
  			log.debug("use SYST_NETWARE");
  		}
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_OS400);
  		}
  		else
  		{
  			return ftp;
  		}
  		if (!isFileNotNull(ftp.listFiles(new String("/*".getBytes("GBK"),"iso_8859_1"))))
  		{
  			ftp.disconnect();
  			ftp = newFTP(ip, port, user, pwd, FTPClientConfig.SYST_VMS);
  		}
  		else
  		{
  			return ftp;
  		}
  		return ftp;
  	}

  	/**
  	 * 根据通配符，列出需要采集的文件目录
  	 * @param collectPath
  	 * @param ip
  	 * @param port
  	 * @param user
  	 * @param pwd
  	 * @param encode
  	 * @param reConnect
  	 * @param parserId
  	 * @return
  	 * @throws Exception
  	 */
	public static List<String> listFTPDirs(String collectPath, String ip, int port, String user, String pwd, String encode, boolean reConnect, int parserId)
    	throws Exception
	{
		List<String> result = new ArrayList();
		String tmp = collectPath.substring(0, collectPath.lastIndexOf("/"));
		if ((!tmp.contains("*")) && (!tmp.contains("?")))
		{
			result.add(collectPath);
			return result;
		}
		if ((parserId == 18) || (parserId == 19) || (parserId == 4001))
		{
			result.add(collectPath);
			return result;
		}
		FTPClient ftp = new FTPClient();
		try
		{
			ftp.connect(ip, port);
			ftp.login(user, pwd);
			ftp = setFTPClientConfig(ftp, ip, port, user, pwd);
			ftp.enterLocalPassiveMode();
			ftp.setFileType(2);
			//ftp.setControlEncoding("GBK");
			ftp.setDataTimeout(3600000);
			ftp.setDefaultTimeout(3600000);
			String[] split = collectPath.split("/");

			List temp = new ArrayList();
			FTPFile[] fs;
			for (int i = 0; i < split.length; i++)
			{
				String s = split[i];
				if (!isNotNull(s))
					continue;
				if (!s.startsWith("/"))
				{
					s = "/" + s;
				}
				String str;
				if (temp.size() > 0)
				{
					List<String> temp2 = new ArrayList(temp);
					temp.clear();
					for (String t : temp2)
					{
						str = t + s;
						if ((!s.contains("*")) && (!s.contains("?")))
						{
							result.add(str);
							temp.add(str);
						}
						else 
						{
							str = isNotNull(encode) ? new String(str.getBytes(encode), "iso_8859_1") : str;
							log.debug("listing - " + str);
						
							fs = ftp.listFiles(str);
							
							//FTPFile[] test1 = ftp.listFiles(new String("/DTFiles/04020701/7/*2011-09-26*".getBytes("UTF8"), "iso_8859_1"));
							//FTPFile[] test2 = ftp.listFiles(new String("/DTFiles/04020701/7/".getBytes("GBK"),"iso_8859_1"));
							
							//log.debug("测试发现文件目录1：" + test1.length);
							//log.debug("测试发现文件目录2：" + test2.length);
							
							int delay;
							if ((fs.length == 0) && (reConnect))
							{
								for (int times = 0; times < 3; times++)
								{
									delay = (times + 1) * 1500;
									Thread.sleep(delay);
									ftp.disconnect();
									Thread.sleep(500L);
									ftp.connect(ip, port);
									ftp.login(user, pwd);
									ftp = setFTPClientConfig(ftp, ip, port, user, pwd);
									ftp.enterLocalPassiveMode();
									ftp.setFileType(2); 
									
									ftp.setDataTimeout(3600000);
									ftp.setDefaultTimeout(3600000);
									fs = ftp.listFiles(str);
									if (fs.length > 0)
									{
										break;
									}
								}
							}
							
							if(fs.length == 0)
							{
								log.debug("未找到目录:" + str);
							}
							
							for (FTPFile f : fs)
							{
								log.debug("找到目录 - " + f.getName() + 
										"    parent - " + str + "  raw - " + 
	        						f.getRawListing());
								if ((!f.isDirectory()) || 
										(f.getName().endsWith(".")) || 
										(f.getName().endsWith("..")))
									continue;
								String name = f.getName();
								name = name.substring(name.lastIndexOf("/") + 1, name.length());
								name = isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
								if (!name.startsWith("/"))
								{
									result.add(t + "/" + name);
									temp.add(t + "/" + name);
								}
								else
								{
									result.add(t + name);
									temp.add(t + name);
								}
							}
	           	 		}
	
					}
	
				}
				else if ((!s.contains("*")) && (!s.contains("?")))
				{
					result.add(s);
					temp.add(s);
				}
				else 
				{
					s = isNotNull(encode) ? new String(s.getBytes(encode), "iso_8859_1") : s;
	
					log.debug("listing(s) - " + s);
					fs = ftp.listFiles(s);
					//log.debug("listFiles-ReplyCode:" + ftp.getReplyCode());
					int delay;
					if ((fs.length == 0) && (reConnect))
					{
						for (int times = 0; times < 3; times++)
						{
							delay = (times + 1) * 1500;
							Thread.sleep(delay);
							ftp.disconnect();
							Thread.sleep(500L);
							ftp.connect(ip, port);
							ftp.login(user, pwd);
							ftp = setFTPClientConfig(ftp, ip, port, user, pwd);
							ftp.enterLocalPassiveMode();
							ftp.setFileType(2);
							ftp.setDataTimeout(3600000);
							ftp.setDefaultTimeout(3600000);
							fs = ftp.listFiles(s);
							if (fs.length > 0)
							{
								break;
							}
						}
						
						if(fs.length == 0)
						{
							log.debug("未找到目录:" + s);
						}
					}
					
					
					for (FTPFile f : fs)
					{
						if ((!f.isDirectory()) || (f.getName().equals(".")) || 
								(f.getName().equals("..")))
							continue;
						String name = f.getName();
						name = isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
						result.add("/" + name);
						temp.add("/" + name);
					}
	
				}
	
			}

			List<String> tmpResult = new ArrayList(result);

			for (String s : result)
			{
				if (s.split("/").length == split.length - 1)
					continue;
				tmpResult.remove(s);
			}

			result.clear();

			for (String s : tmpResult)
			{
				result.add(s + "/" + split[(split.length - 1)]);
			}

		}
		finally
		{
			ftp.disconnect();
		}
		return result;
	}

  	public static List<String> listFTPDirs(String collectPath, String ip, int port, String user, String pwd, String encode, int parserId)
  		throws Exception
	{
  		return listFTPDirs(collectPath, ip, port, user, pwd, encode, true, parserId);
	}
  	
  	public static List<String> listFTPDirs(String collectPath, String ip, int port, String user, String pwd, String encode, boolean reConnect)
  		throws Exception
  	{
  		List<String> result = new ArrayList();
  		String tmp = collectPath.substring(0, collectPath.lastIndexOf("/"));
  		if ((!tmp.contains("*")) && (!tmp.contains("?")))
  		{
  			result.add(collectPath);
  			return result;
  		}	
  		FTPClient ftp = new FTPClient();
  		try
  		{
  			ftp.connect(ip, port);
  			ftp.login(user, pwd);
  			ftp = setFTPClientConfig(ftp, ip, port, user, pwd);
  			ftp.enterLocalPassiveMode();
  			ftp.setFileType(2);

  			ftp.setDataTimeout(3600000);
  			ftp.setDefaultTimeout(3600000);
  			String[] split = collectPath.split("/");

  			List temp = new ArrayList();
  			FTPFile[] fs;
  			for (int i = 0; i < split.length; i++)
  			{
  				String s = split[i];
  				if (!isNotNull(s))
  					continue;
  				if (!s.startsWith("/"))
  				{
  					s = "/" + s;
  				}
  				String str;
  				if (temp.size() > 0)
  				{
  					List<String> temp2 = new ArrayList(temp);
  					temp.clear();
  					for (String t : temp2)
  					{
  						str = t + s;
  						if ((!s.contains("*")) && (!s.contains("?")))
  						{
  							result.add(str);
  							temp.add(str);
  						}
  						else {
  							str = isNotNull(encode) ? new String(str.getBytes(encode), "iso_8859_1") : str;
  							fs = ftp.listFiles(str);
  							int delay;
  							if ((fs.length == 0) && (reConnect))
  							{
  								for (int times = 0; times < 3; times++)
  								{
  									delay = (times + 1) * 1500;
  									Thread.sleep(delay);
  									ftp.disconnect();
  									Thread.sleep(500L);
  									ftp.connect(ip, port);
  									ftp.login(user, pwd);
  									setFTPClientConfig(ftp, ip, port, user, pwd);
  									ftp.enterLocalPassiveMode();
  									ftp.setFileType(2);
  									ftp.setDataTimeout(3600000);
  									ftp.setDefaultTimeout(3600000);
  									fs = ftp.listFiles(str);
  									if (fs.length > 0)
  									{
  										break;
  									}
  								}
  							}
  							for (FTPFile f : fs)
  							{
  								if ((!f.isDirectory()) || 
  										(f.getName().endsWith(".")) || 
  										(f.getName().endsWith("..")))
  									continue;
  								String name = f.getName();
  								name = name.substring(name.lastIndexOf("/") + 1, name.length());
  								name = isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
  								if (!name.startsWith("/"))
  								{
  									result.add(t + "/" + name);
  									temp.add(t + "/" + name);
  								}
  								else
  								{
  									result.add(t + name);
  									temp.add(t + name);
  								}
  							}
  						}

  					}

  				}
  				else if ((!s.contains("*")) && (!s.contains("?")))
  				{
  					result.add(s);
  					temp.add(s);
  				}
  				else {
  					s = isNotNull(encode) ? new String(s.getBytes(encode), "iso_8859_1") : s;
  					fs = ftp.listFiles(s);
  					int delay;
  					if ((fs.length == 0) && (reConnect))
  					{
  						for (int times = 0; times < 3; times++)
  						{
  							delay = (times + 1) * 1500;
  							Thread.sleep(delay);
  							ftp.disconnect();
  							Thread.sleep(500L);
  							ftp.connect(ip, port);
  							ftp.login(user, pwd);
  							setFTPClientConfig(ftp, ip, port, user, pwd);
  							ftp.enterLocalPassiveMode();
              				ftp.setFileType(2);
              				ftp.setDataTimeout(3600000);
              				ftp.setDefaultTimeout(3600000);
              				fs = ftp.listFiles(s);
              				if (fs.length > 0)
              				{
              					break;
              				}
  						}
  					}
  					for (FTPFile f : fs)
  					{
  						if ((!f.isDirectory()) || (f.getName().equals(".")) || 
  								(f.getName().equals("..")))
  							continue;
  						String name = f.getName();
  						name = isNotNull(encode) ? new String(name.getBytes("iso_8859_1"), encode) : name;
  						result.add("/" + name);
  						temp.add("/" + name);
  					}

  				}

  			}

  			List<String> tmpResult = new ArrayList(result);

  			for (String s : result)
  			{
  				if (s.split("/").length == split.length - 1)
  					continue;
  				tmpResult.remove(s);
  			}

  			result.clear();

  			for (String s : tmpResult)
  			{
  				result.add(s + "/" + split[(split.length - 1)]);
  			}

  		}
  		finally
  		{
  			ftp.disconnect();
  		}
  		return result;
  	}

  public static Double isOracleNumberString(String str)
  {
    if (isNull(str))
      return null;
    String s = str.trim();
    try
    {
      Double dou = Double.valueOf(Double.parseDouble(s));
      return dou;
    }
    catch (Exception unused) {
    }
    return null;
  }

  public static void closeCloseable(Closeable closeable)
  {
    if (closeable != null)
    {
      try
      {
        closeable.close();
      }
      catch (IOException localIOException)
      {
      }
    }
  }

  public static String nvl(String str, String replace)
  {
    return isNull(str) ? replace : str;
  }
  
  /**
   * Double 精确小数点位数
   * @param value
   * @param scale
   * @param roundingMode
   * @return
   */
  public static double round(double value, int scale, int roundingMode) {   
      BigDecimal bd = new BigDecimal(value);   
      bd = bd.setScale(scale, roundingMode);   
      double d = bd.doubleValue();   
      bd = null;   
      return d;   
  }   
  
  /**
   * 文件拷贝
   * @param sourceFile 源文件目录
   * @param targetFile 目标文件目录
   */
  public static boolean FileCopy(String sourceFile,String targetFile)
  {
	//拷贝文件
		InputStream is=null; 
		OutputStream os=null; 
		boolean result = false;
		try 
		{ 

			is=new BufferedInputStream(new FileInputStream(sourceFile)); 
			os=new BufferedOutputStream(new FileOutputStream(targetFile)); 


			byte[] b=new byte[65535]; 
			int len=0; 
			String str=null; 
			StringBuilder sb=new StringBuilder(); 
			try 
			{ 
				while((len=is.read(b))!=-1)
				{ 
					os.write(b,0,len); 
				} 
				os.flush(); 
				result = true;
			} 
			catch (IOException e) 
			{ 
			// TODO Auto-generated catch block 
				e.printStackTrace(); 
				result = false;
			}
			finally
			{
				try 
				{ 
					if(is!=null)
					{ 
						is.close();
					}
					
				} 
				catch (IOException e) 
				{ 
					// TODO Auto-generated catch block 
					//e.printStackTrace(); 
					log.error(String.format("拷贝文件失败:%s->%s;失败原因:%s"
							,sourceFile,targetFile,e.getMessage()));
				} 
			}
		} 
		catch (FileNotFoundException e) 
		{ 
			//e.printStackTrace();
			log.error(String.format("拷贝文件失败:%s->%s;失败原因:%s"
					,sourceFile,targetFile,e.getMessage()));
			result = false;
		}
		finally
		{ 
			if(os!=null)
			{ 
				try 
				{ 
					os.close(); 
				} 
				catch (IOException e) 
				{ 
					//e.printStackTrace();
					log.error(String.format("拷贝文件失败:%s->%s;失败原因:%s"
							,sourceFile,targetFile,e.getMessage()));
					result = false;
				} 
			} 
		} 
		return result;
  }
  
 
}