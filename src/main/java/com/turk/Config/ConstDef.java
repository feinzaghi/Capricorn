package com.turk.Config;

import java.io.BufferedReader;
import java.io.File;
import java.io.PrintStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Clob;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import com.turk.util.Util;

public class ConstDef
{
	public static final int MAX_RETRY_NUM = 5;
	public static final int COLLECT_TYPE_TELNET = 1;
	public static final int COLLECT_TYPE_TCP = 2;
	public static final int COLLECT_TYPE_FTP = 3;
	public static final int COLLECT_TYPE_FILE = 4;
	public static final int COLLECT_TYPE_DataBase = 5;
  	public static final int COLLECT_TYPE_CORBA = 6;
  	public static final int COLLECT_TYPE_FTP_DOWNLOADER = 9;
  	public static final String ESPECIAL_CHAR = "\"";
  	public static final int PARSE_TYPE_SOCKET_M2000 = 8;
  	public static final int COLLECT_PERIOD_FOREVER = 1;
  	public static final int COLLECT_PERIOD_DAY = 2;
  	public static final int COLLECT_PERIOD_HOUR = 3;
  	public static final int COLLECT_PERIOD_MINUTE_HALFHOUR = 4;
  	public static final int COLLECT_PERIOD_MINUTE_QUARTER = 5;
  	public static final int COLLECT_PERIOD_4HOUR = 6;
  	public static final int COLLECT_PERIOD_5MINUTE = 7;
  	public static final int COLLECT_PERIOD_HALFDAY = 8;
  	public static final int COLLECT_PERIOD_10MINUTE = 10;
  	public static final int COLLECT_TEMPLATE_X = 999;
  	public static final int COLLECT_TEMPLATE_NULL = 0;
  	public static final int COLLECT_TEMPLATE_LINE = 1;
  	public static final int COLLECT_TEMPLATE_SECT = 2;
  	public static final int COLLECT_TEMPLATE_THRD = 3;
  	public static final int COLLECT_TEMPLATE_XML = 4;
  	public static final int COLLECT_TEMPLATE_XLS = 5;
  	public static final int COLLECT_TEMPLATE_LUCENT_EVDO = 11;
  	public static final int COLLECT_TEMPLATE_HUAWEI_FTP = 12;
  	public static final int COLLECT_TEMPLATE_PM_ZTE = 13;
  	public static final int COLLECT_TEMPLATE_HUAWEI_MML = 14;
  	public static final int COLLECT_TEMPLATE_HUAWEI_M2000 = 15;
  	public static final int COLLECT_TEMPLATE_HUAWEI_CM_CSV = 16;
  	public static final int COLLECT_TEMPLATE_HUAWEI_CONFIG = 17;
  	public static final int COLLECT_TEMPLATE_HUAWEI_ALARM_SH = 2001;
  	public static final int COLLECT_TEMPLATE_ERIC_PM = 18;
  	public static final int COLLECT_TEMPLATE_ERIC_CM = 19;
  	public static final int COLLECT_TEMPLATE_ERIC_V1_CM = 23;
  	public static final int COLLECT_TEMPLATE_HW_DBF = 35;
  	public static final int COLLECT_TEMPLATE_HUAWEI_AM = 20;
  	public static final int COLLECT_TEMPLATE_SECT_21 = 21;
  	public static final int COLLECT_TEMPLATE_GPS_ENSURE_POS = 24;
  	public static final int COLLECT_DATA_BUFF_SIZE = 1048576;
  	public static final int COLLECT_LOGTYPE_NATURAL = 1;
  	public static final int COLLECT_LOGTYPE_ERROR = 2;
  	public static final int COLLECT_SECT_SCANTYPE_N = 1;
  	public static final int COLLECT_SECT_SCANTYPE_KEYWORD = 2;
  	public static final int COLLECT_SECT_SCANTYPE_SPLIT_KEYWORD = 3;
  	public static final int COLLECT_SECT_KEYWORD_HEAD = 1;
  	public static final int COLLECT_SECT_KEYWORD_TAIL = 2;
  	public static final int COLLECT_SECT_KEYWORD_SENTER = 3;
  	public static final int COLLECT_SECT_PARSE_BITPOS = 1;
  	public static final int COLLECT_SECT_PARSE_KEYWORD = 2;
  	public static final int COLLECT_SECT_PARSE_TOEND = 3;
  	public static final int COLLECT_SECT_PARSE_KEYFIELDONLYONE = 4;
  	public static final int COLLECT_SECT_PARSE_COMPLEX = 10;
  	public static final int COLLECT_SECT_PARSE_SPLIT = 11;
  	public static final int COLLECT_SECT_PARSE_SPLIT_LINE = 12;
  	public static final int COLLECT_LINE_PARSE_SPLIT = 1;
  	public static final int COLLECT_LINE_PARSE_BITPOS = 2;
  	public static final int COLLECT_LINE_PARSE_RAW = 3;
  	public static final int COLLECT_LINE_PARSE_FREEDOM = 4;
  	public static final int COLLECT_FIELD_DATATYPE_DIGITAL = 1;
  	public static final int COLLECT_FIELD_DATATYPE_STRING = 2;
  	public static final int COLLECT_FIELD_DATATYPE_DATATIME = 3;
  	public static final int COLLECT_FIELD_DATATYPE_LOB = 4;
  	public static final int COLLECT_DISTRIBUTE_INSERT = 1;
  	public static final int COLLECT_DISTRIBUTE_SQLLDR = 2;
  	public static final int COLLECT_DISTRIBUTE_SQLLDR_DYNAMIC = 3;
  	public static final int COLLECT_DISTRIBUTE_FILE = 4;
  	public static final int MR_SOURCE_ZCTT = 0;
	public static final int MR_SOURCE_ZTE1 = 1;
	public static final int MR_SOURCE_ZTE2 = 2;
	public static final int MR_SOURCE_MOTO = 3;

	public static String getExpression(String str)
	{
		String result = null;

		int b = str.indexOf("(");
		int e = str.indexOf(")");

		if ((b > 0) && (e > b))
		{
			result = str.substring(b + 1, e);
		}

		if ((result != null) && (!result.contains("%%"))) {
			return null;
		}
		return result;
	}

	public static String ParseFilePath(String strPath, Timestamp timestamp)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");

		int iHwrIdx = strPath.indexOf("%%TA");
		int iDiff = 0;
		if (iHwrIdx >= 0)
		{
			iDiff = Integer.parseInt(strPath.substring(iHwrIdx + 4, iHwrIdx + 5));
			strPath = strPath.replaceAll("%%TA.", "");
		}

		timestamp = new Timestamp(timestamp.getTime() + iDiff * 3600 * 1000);
		String strTime = formatter.format(timestamp);

		if (strPath.indexOf("%%Y") >= 0)
			strPath = strPath.replace("%%Y", strTime.substring(0, 4));
		Calendar calendar = Calendar.getInstance();

		Date date = new Date();
		date.setTime(timestamp.getTime());
		calendar.setTime(new Date());

		calendar.setTime(date);
		int nDayOrYear = calendar.get(6);

		if (strPath.indexOf("%%WEEK") >= 0)
		{
			int dow = calendar.get(7);
			dow--;
			if (dow == 0)
				dow = 7;
			strPath = strPath.replace("%%WEEK", String.valueOf(dow));
		}

		if (nDayOrYear < 10)
			strPath = strPath.replace("%%DayOfYear", "00" + nDayOrYear);
		else if (nDayOrYear < 100)
			strPath = strPath.replace("%%DayOfYear", "0" + nDayOrYear);
		else {
			strPath = strPath.replace("%%DayOfYear", String.valueOf(nDayOrYear));
		}
		if (strPath.indexOf("%%y") >= 0) {
			strPath = strPath.replace("%%y", strTime.substring(2, 4));
		}
		if (strPath.indexOf("%%EM") >= 0)
		{
			switch (Integer.parseInt(strTime.substring(4, 6)))
			{
				case 1:
					strPath = strPath.replace("%%EM", "Jan");
					break;
				case 2:
					strPath = strPath.replace("%%EM", "Feb");
					break;
				case 3:
					strPath = strPath.replace("%%EM", "Mar");
					break;
				case 4:
					strPath = strPath.replace("%%EM", "Apr");
					break;
				case 5:
					strPath = strPath.replace("%%EM", "May");
					break;
				case 6:
					strPath = strPath.replace("%%EM", "Jun");
					break;
				case 7:
					strPath = strPath.replace("%%EM", "Jul");
					break;
				case 8:
					strPath = strPath.replace("%%EM", "Aug");
					break;
				case 9:
					strPath = strPath.replace("%%EM", "Sep");
					break;
				case 10:
					strPath = strPath.replace("%%EM", "Oct");
					break;
				case 11:
					strPath = strPath.replace("%%EM", "Nov");
					break;
				case 12:
					strPath = strPath.replace("%%EM", "Dec");
			}

		}

		if (strPath.indexOf("%%M") >= 0) {
			strPath = strPath.replace("%%M", strTime.substring(4, 6));
		}
		if (strPath.indexOf("%%d") >= 0) {
			strPath = strPath.replace("%%d", strTime.substring(6, 8));
		}
		if (strPath.indexOf("%%D") >= 0) {
			strPath = strPath.replace("%%D", strTime.substring(6, 8));
		}

		if (strPath.indexOf("%%fd") >= 0)
		{
			strPath = strPath.replace("%%fd", strTime.substring(6, 8));
		}
		String sd = null;
		if (strPath.indexOf("%%FD") >= 0)
		{
			sd = strTime.substring(6, 8);
			try
			{
				if (Integer.valueOf(sd).intValue() < 10)
				{
					strPath = strPath.replace("%%FD", strTime.substring(7, 8));
				}
				else
					strPath = strPath.replace("%%FD", strTime.substring(6, 8));
			}
			catch (NumberFormatException e)
			{
				e.printStackTrace();
			}
		}
		String fh = null;
		if (strPath.indexOf("%%FH") >= 0)
		{
			String strHour = getExpression(strPath);
			if ((strHour != null) && (!strHour.equals("")))
			{
				String strHourTmp = strHour.replaceAll("%%FH", strTime.substring(8, 10));
				int nHour = com.turk.util.Util.parseExpression(strHourTmp);
        
				if (nHour > 23) {
					nHour = 0;
				}
				strPath = strPath.replace("(" + strHour + ")", String.valueOf(nHour));
				if (nHour < 10)
					strPath = strPath.replaceAll("%%FH", strTime.substring(7, 10));
				else
					strPath = strPath.replaceAll("%%FH", strTime.substring(8, 10));
			}
			else
			{
				fh = strTime.substring(8, 10);
				if (Integer.valueOf(fh).intValue() < 10)
				{
					strPath = strPath.replace("%%FH", strTime.substring(9, 10));
				}
				else {
					strPath = strPath.replace("%%FH", strTime.substring(8, 10));
				}
			}
		}

		if (strPath.indexOf("%%H") >= 0)
		{
			String strHour = getExpression(strPath);
			if ((strHour != null) && (!strHour.equals("")))
			{
				String strHourTmp = strHour.replaceAll("%%H", strTime.substring(8, 10));
				int nHour = com.turk.util.Util.parseExpression(strHourTmp);

				if (nHour > 23) {
					nHour = 0;
				}

				strPath = strPath.replace("(" + strHour + ")", com.turk.util.Util.trimHour(nHour));
				strPath = strPath.replaceAll("%%H", strTime.substring(8, 10));
			}
			else
			{
				strPath = strPath.replaceAll("%%H", strTime.substring(8, 10));
			}
		}

		if (strPath.indexOf("%%h") >= 0) {
			strPath = strPath.replaceAll("%%h", strTime.substring(8, 10));
		}
		if (strPath.indexOf("%%m") >= 0) {
			strPath = strPath.replace("%%m", strTime.substring(10, 12));
		}
		//只取十位数
		if (strPath.indexOf("%%Tm") >= 0) {
			strPath = strPath.replace("%%Tm", strTime.substring(10, 11));
		}
		
		if (strPath.indexOf("%%s") >= 0) {
			strPath = strPath.replace("%%s", strTime.substring(12, 14));
		}
		if (strPath.indexOf("%%S") >= 0) {
			strPath = strPath.replace("%%S", strTime.substring(12, 14));
		}
		String strInterval = "";
		int nInterval = 0;
		if (strPath.indexOf("|") > 0)
		{
			strInterval = strPath.substring(strPath.indexOf("|") + 1);
			strPath = strPath.substring(0, strPath.indexOf("|"));

			nInterval = Integer.parseInt(strInterval);
			timestamp = new Timestamp(timestamp.getTime() + nInterval);
			strTime = formatter.format(timestamp);

			calendar.setTime(timestamp);

			if (strPath.indexOf("%%NWEEK") >= 0)
			{
				int dow = calendar.get(7);
				dow--;
				if (dow == 0)
					dow = 7;
				strPath = strPath.replace("%%NWEEK", String.valueOf(dow));
			}

			if (strPath.indexOf("%%NY") >= 0) {
				strPath = strPath.replace("%%NY", strTime.substring(0, 4));
			}
			if (strPath.indexOf("%%Ny") >= 0) {
				strPath = strPath.replace("%%Ny", strTime.substring(2, 4));
			}
			if (strPath.indexOf("%%NEM") >= 0)
			{
				switch (Integer.parseInt(strTime.substring(4, 6)))
				{
					case 1:
						strPath = strPath.replace("%%NEM", "Jan");
						break;
					case 2:
						strPath = strPath.replace("%%NEM", "Feb");
						break;
					case 3:
						strPath = strPath.replace("%%NEM", "Mar");
						break;
					case 4:
						strPath = strPath.replace("%%NEM", "Apr");
						break;
					case 5:
						strPath = strPath.replace("%%NEM", "May");
						break;
					case 6:
						strPath = strPath.replace("%%NEM", "Jun");
						break;
					case 7:
						strPath = strPath.replace("%%NEM", "Jul");
						break;
					case 8:
						strPath = strPath.replace("%%NEM", "Aug");
						break;
					case 9:
						strPath = strPath.replace("%%NEM", "Sep");
						break;
					case 10:
						strPath = strPath.replace("%%NEM", "Oct");
						break;
					case 11:
						strPath = strPath.replace("%%NEM", "Nov");
						break;
					case 12:
						strPath = strPath.replace("%%NEM", "Dec");
				}

			}

			if (strPath.indexOf("%%NM") >= 0) {
				strPath = strPath.replace("%%NM", strTime.substring(4, 6));
			}
			if (strPath.indexOf("%%Nd") >= 0) {
				strPath = strPath.replace("%%Nd", strTime.substring(6, 8));
			}
			if (strPath.indexOf("%%ND") >= 0) {
				strPath = strPath.replace("%%ND", strTime.substring(6, 8));
			}
			if (strPath.indexOf("%%NH") >= 0) {
				strPath = strPath.replace("%%NH", strTime.substring(8, 10));
			}
			if (strPath.indexOf("%%NV4") >= 0)
			{
				int nNum = Integer.parseInt(strTime.substring(8, 10));
				nNum = (nNum + 1) / 4;
				strPath = strPath.replace("%%NV4", "0" + nNum);
			}

			if (strPath.indexOf("%%Nh") >= 0) {
				strPath = strPath.replace("%%Nh", strTime.substring(8, 10));
			}
			if (strPath.indexOf("%%Nm") >= 0) {
				strPath = strPath.replace("%%Nm", strTime.substring(10, 12));
			}
			
			
			
			if (strPath.indexOf("%%Ns") >= 0) {
				strPath = strPath.replace("%%Ns", strTime.substring(12, 14));
			}
			if (strPath.indexOf("%%NS") >= 0) {
				strPath = strPath.replace("%%NS", strTime.substring(12, 14));
			}
		}
		return strPath;
	}

	public static String ParseFilePathForDB(String strPath, Timestamp timestamp)
	{
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");

		int iHwrIdx = strPath.indexOf("%%TA");
		int iDiff = 0;
    	if (iHwrIdx >= 0)
    	{
    		iDiff = Integer.parseInt(strPath.substring(iHwrIdx + 4, iHwrIdx + 5));
    		strPath = strPath.replaceAll("%%TA.", "");
    	}

    	timestamp = new Timestamp(timestamp.getTime() + iDiff * 3600 * 1000);
    	String strTime = formatter.format(timestamp);

    	if (strPath.indexOf("%%Y") >= 0)
    		strPath = strPath.replace("%%Y", strTime.substring(0, 4));
    	Calendar calendar = Calendar.getInstance();

    	Date date = new Date();
    	date.setTime(timestamp.getTime());
    	calendar.setTime(new Date());

    	calendar.setTime(date);
    	int nDayOrYear = calendar.get(6);

    	if (nDayOrYear < 10)
    		strPath = strPath.replace("%%DayOfYear", "00" + nDayOrYear);
    	else if (nDayOrYear < 100)
    		strPath = strPath.replace("%%DayOfYear", "0" + nDayOrYear);
    	else {
    		strPath = strPath.replace("%%DayOfYear", String.valueOf(nDayOrYear));
    	}
    	if (strPath.indexOf("%%y") >= 0) {
    		strPath = strPath.replace("%%y", strTime.substring(2, 4));
    	}
    	if (strPath.indexOf("%%EM") >= 0)
    	{
    		switch (Integer.parseInt(strTime.substring(4, 6)))
    		{
    			case 1:
    				strPath = strPath.replace("%%EM", "Jan");
    				break;
    			case 2:
    				strPath = strPath.replace("%%EM", "Feb");
    				break;
    			case 3:
    				strPath = strPath.replace("%%EM", "Mar");
    				break;
    			case 4:
    				strPath = strPath.replace("%%EM", "Apr");
    				break;
    			case 5:
    				strPath = strPath.replace("%%EM", "May");
    				break;
    			case 6:
    				strPath = strPath.replace("%%EM", "Jun");
    				break;
    			case 7:
    				strPath = strPath.replace("%%EM", "Jul");
    				break;
    			case 8:
    				strPath = strPath.replace("%%EM", "Aug");
    				break;
    			case 9:
    				strPath = strPath.replace("%%EM", "Sep");
    				break;
    			case 10:
    				strPath = strPath.replace("%%EM", "Oct");
    				break;
    			case 11:
    				strPath = strPath.replace("%%EM", "Nov");
    				break;
    			case 12:
    				strPath = strPath.replace("%%EM", "Dec");
    		}

    	}

    	if (strPath.indexOf("%%M") >= 0) {
    		strPath = strPath.replace("%%M", strTime.substring(4, 6));
    	}
    	if (strPath.indexOf("%%d") >= 0) {
    		strPath = strPath.replace("%%d", strTime.substring(6, 8));
    	}
    	if (strPath.indexOf("%%D") >= 0) {
    		strPath = strPath.replace("%%D", strTime.substring(6, 8));
    	}
    	if (strPath.indexOf("%%H") >= 0)
    	{
    		strPath = strPath.replace("%%H", strTime.substring(8, 10));
    	}

    	if (strPath.indexOf("%%h") >= 0) {
    		strPath = strPath.replace("%%h", strTime.substring(8, 10));
    	}
    	if (strPath.indexOf("%%m") >= 0) {
    		strPath = strPath.replace("%%m", strTime.substring(10, 12));
    	}
    	if (strPath.indexOf("%%s") >= 0) {
    		strPath = strPath.replace("%%s", strTime.substring(12, 14));
    	}
    	if (strPath.indexOf("%%S") >= 0) {
    		strPath = strPath.replace("%%S", strTime.substring(12, 14));
    	}
    	String strInterval = "";
    	int nInterval = 0;
    	if (strPath.indexOf("|") > 0)
    	{
    		strInterval = strPath.substring(strPath.indexOf("|") + 1);
    		strPath = strPath.substring(0, strPath.indexOf("|"));

    		nInterval = Integer.parseInt(strInterval);
    		timestamp = new Timestamp(timestamp.getTime() + nInterval);
    		strTime = formatter.format(timestamp);

    		if (strPath.indexOf("%%NY") >= 0) {
    			strPath = strPath.replace("%%NY", strTime.substring(0, 4));
    		}
    		if (strPath.indexOf("%%Ny") >= 0) {
    			strPath = strPath.replace("%%Ny", strTime.substring(2, 4));
    		}
    		if (strPath.indexOf("%%NEM") >= 0)
    		{
    			switch (Integer.parseInt(strTime.substring(4, 6)))
    			{
    			case 1:
    				strPath = strPath.replace("%%NEM", "Jan");
    				break;
    			case 2:
    				strPath = strPath.replace("%%NEM", "Feb");
    				break;
    			case 3:
    				strPath = strPath.replace("%%NEM", "Mar");
    				break;
    			case 4:
    				strPath = strPath.replace("%%NEM", "Apr");
    				break;
    			case 5:
    				strPath = strPath.replace("%%NEM", "May");
    				break;
    			case 6:
    				strPath = strPath.replace("%%NEM", "Jun");
    				break;
    			case 7:
    				strPath = strPath.replace("%%NEM", "Jul");
    				break;
    			case 8:
    				strPath = strPath.replace("%%NEM", "Aug");
    				break;
    			case 9:
    				strPath = strPath.replace("%%NEM", "Sep");
    				break;
    			case 10:
    				strPath = strPath.replace("%%NEM", "Oct");
    				break;
    			case 11:
    				strPath = strPath.replace("%%NEM", "Nov");
    				break;
    			case 12:
    				strPath = strPath.replace("%%NEM", "Dec");
    			}

    		}

    		if (strPath.indexOf("%%NM") >= 0) {
    			strPath = strPath.replace("%%NM", strTime.substring(4, 6));
    		}
    		if (strPath.indexOf("%%Nd") >= 0) {
    			strPath = strPath.replace("%%Nd", strTime.substring(6, 8));
    		}
    		if (strPath.indexOf("%%ND") >= 0) {
    			strPath = strPath.replace("%%ND", strTime.substring(6, 8));
    		}
    		if (strPath.indexOf("%%NH") >= 0) {
    			strPath = strPath.replace("%%NH", strTime.substring(8, 10));
    		}
    		if (strPath.indexOf("%%NV4") >= 0)
    		{
    			int nNum = Integer.parseInt(strTime.substring(8, 10));
    			nNum = (nNum + 1) / 4;
    			strPath = strPath.replace("%%NV4", "0" + nNum);
    		}

    		if (strPath.indexOf("%%Nh") >= 0) {
    			strPath = strPath.replace("%%Nh", strTime.substring(8, 10));
    		}
    		if (strPath.indexOf("%%Nm") >= 0) {
    			strPath = strPath.replace("%%Nm", strTime.substring(10, 12));
    		}
    		if (strPath.indexOf("%%Ns") >= 0) {
    			strPath = strPath.replace("%%Ns", strTime.substring(12, 14));
    		}
    		if (strPath.indexOf("%%NS") >= 0) {
    			strPath = strPath.replace("%%NS", strTime.substring(12, 14));
    		}
    	}
    	return strPath;
	}

	public static String ClobParse(Clob clob)
	{
		if (clob == null) return "";
		StringBuilder sb = new StringBuilder();
		try
		{
			Reader is = clob.getCharacterStream();
			BufferedReader br = new BufferedReader(is);
			String s = null;

			while ((s = br.readLine()) != null)
			{
				sb.append(s);
			}

			br.close();
			is.close();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return sb.toString();
	}

	public static String CreateFolder(String strCurrentPath, int TaskID, String strFileName)
	{
		String fn = strFileName;

		if (com.turk.util.Util.isNotNull(strFileName))
		{
			if ((strFileName.contains("!")) && (strFileName.contains("{")) && 
					(strFileName.contains("}")))
			{
				int begin = strFileName.indexOf("!");
				int end = strFileName.lastIndexOf("!");
				if ((begin > -1) && (end > -1) && (begin < end))
				{
					String content = strFileName.substring(begin, end + 1);
					int cBegin = content.indexOf("{");
					int cEnd = content.indexOf("}");
					if ((cBegin > -1) && (cEnd > -1) && (cBegin < cEnd))
					{
						String dir = content.substring(cBegin + 1, cEnd);
						fn = fn.replace(content, dir);
					}
				}
			}
		}

		if (!new File(strCurrentPath).isDirectory())
		{
			new File(strCurrentPath).mkdir();
		}
		String[] strFolderList = fn.split("/");

		for (int i = 0; i < strFolderList.length - 1; i++)
		{
			if (strFolderList[i].equals(""))
				continue;
			strFolderList[i] = strFolderList[i].replaceAll("\\W", "_");
			strCurrentPath = strCurrentPath + File.separatorChar + 
			strFolderList[i];
			if (new File(strCurrentPath).isDirectory())
				continue;
			new File(strCurrentPath).mkdir();
		}

		return strCurrentPath;
	}

	public static String createLocalFolder(String strCurrentPath, int TaskID, String strFileName)
	{
		File f = new File(strCurrentPath);
		if (!f.exists()) {
			f.mkdirs();
		}
		String[] strFolderList = strFileName.split("/");

		for (int i = 0; i < strFolderList.length; i++)
		{
			if (strFolderList[i].equals(""))
				continue;
			try
			{
				strFolderList[i] = new String(strFolderList[i].getBytes("GBK"), "iso-8859-1");
			}
			catch (UnsupportedEncodingException e)
			{
				e.printStackTrace();
			}

			strFolderList[i] = strFolderList[i].replaceAll("\\W", "_");

			strCurrentPath = strCurrentPath + File.separatorChar + 
			strFolderList[i];
			new File(strCurrentPath).mkdirs();
		}
		return strCurrentPath;
	}
  
	/**
	 * 以设备编号创建目录
	 * @param strCurrentPath
	 * @param DeviceID
	 * @param strFileName
	 * @return
	 */
	public static String CreateFolder(String strCurrentPath, 
		  String DeviceID, String strFileName)
	{
	    if (!new File(strCurrentPath).isDirectory())
	    {
	      new File(strCurrentPath).mkdir();
	    }
	    String[] strFolderList = strFileName.split("/");
	    int Start = 0;
	    for (int i = 0; i < strFolderList.length - 1; i++)
	    {
	    	 if(strFolderList[i].contains(DeviceID))
	    	 {
	    		 Start = i;
	    	 }
	    }
	    for (int i = Start; i < strFolderList.length - 1; i++)
	    {
	    	if (strFolderList[i].equals(""))
		      continue;
		   
		    	  
		    strFolderList[i] = strFolderList[i].replaceAll("\\W", "_");
		    strCurrentPath = strCurrentPath + File.separatorChar + 
		        strFolderList[i];
		    if (new File(strCurrentPath).isDirectory())
		        continue;
		    new File(strCurrentPath).mkdir();
	    }
	
	    return strCurrentPath;
	}
  
	/**
	 * 在指定路径下创建文件夹,返回创建的文件夹路径
	 * @param strCurrentPath  指定路径
	 * @param strFileName 文件夹名称
	 * @return
	 */
	public static String CreateFolder(String strCurrentPath,String strFileName)
	{
	    if (!new File(strCurrentPath).isDirectory())
	    {
	      new File(strCurrentPath).mkdir();
	    }
	    String[] strFolderList = strFileName.split("/");
	    int Start = 0;
	
	    for (int i = 0; i < strFolderList.length; i++)
	    {
	    	if (strFolderList[i].equals(""))
		      continue;
		   
		    	  
		    strFolderList[i] = strFolderList[i].replaceAll("\\W", "_");
		    strCurrentPath = strCurrentPath + File.separatorChar + 
		        strFolderList[i];
		    if (new File(strCurrentPath).isDirectory())
		        continue;
		    new File(strCurrentPath).mkdir();
	    }
	
	    return strCurrentPath;
	}

	public static void main(String[] args)
	{
		try
		{
			Date d = com.turk.util.Util.getDate1("2010-12-2 1:00:00");

			String s = ParseFilePath("/fileint/pmneexport/neexport_%%Y%%M%%D/ZZR20(%%H+1)/A%%Y%%M%%D.%%H00+0800-*.xm", new Timestamp(d.getTime()));
			System.out.print(s);
		}
		catch (ParseException e)
		{
			e.printStackTrace();
		}
	}
}