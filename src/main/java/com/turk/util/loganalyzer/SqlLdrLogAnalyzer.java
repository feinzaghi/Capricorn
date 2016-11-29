package com.turk.util.loganalyzer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import com.turk.util.SqlldrResult;

public class SqlLdrLogAnalyzer
  	implements LogAnalyzer
{
	private static Map<String, String> configMap = new HashMap();

	private List<String> matchRuleList = new ArrayList();

	private static List<String> uerDefineRuleList = new ArrayList();

	private static String configPath = "SqlLdrLogAnalyseTemplet.xml";

	private static int fileSizeValue = 8388608;
	
	public static void main(String[] args)
	{
		SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
		try {
			SqlldrResult result = 
				analyzer.analysis(new FileInputStream("D:\\temp\\20130514_04020802_4_20130514180103.cti_755_CDMADO.log"));
			double runTime = result.getRunTime();
			
				System.out.print(runTime);
			
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (LogAnalyzerException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static
	{
		try
    	{
			load();
    	}
		catch (Exception e)
		{
			configMap = null;
			uerDefineRuleList = null;
			e.printStackTrace();
		}
	}

	/**
	 * 分析
	 */
	public SqlldrResult analysis(String fileName)
    	throws LogAnalyzerException
    {
		try
		{
			return analysis(new FileInputStream(fileName));
		}
		catch (FileNotFoundException e) {
			throw new LogAnalyzerException("文件未找到", e);
		}
    }

	public SqlldrResult analysis(InputStream in)
    	throws LogAnalyzerException
    {
		if (configMap == null)
		{
			if (in != null)
			{
				try
				{
					in.close();
				}
				catch (IOException localIOException)
				{
				}
			}
			return null;
		}

		SqlldrResult sqlLoadResult = new SqlldrResult();
		try
		{
			int length = in.available();
			String fileSize = (String)configMap.get("maxFileSize");
			if ((fileSize != null) && (!fileSize.trim().equals("")))
			{
				fileSizeValue = Integer.parseInt(fileSize) * 1024;
			}
			byte[] buffer = new byte[length];

			boolean isOracleLog = isOracleLog(in);
			String content = "";
			//当日志文件大于一定值，将不分析文件
			if (length < fileSizeValue)
			{
				if (!isOracleLog)
				{
					return sqlLoadResult;
				}

				in.read(buffer, 0, buffer.length);
				content = new String(buffer);
				sqlLoadResult = getSqlldrAnalyseResult(content, isOracleLog, fileSizeValue);
			}
			content = null;
		}
		catch (Exception e)
		{
			e.printStackTrace();
			try
			{
				if (in != null)
					in.close();
			}
			catch (IOException localIOException2)
			{
			}
		}
		finally
		{
			try
			{
				if (in != null)
					in.close();
			}
			catch (IOException localIOException3)
			{
			}
		}
		return sqlLoadResult;
    }

	public void destory()
	{
		configMap = null;
    	this.matchRuleList = null;
    	uerDefineRuleList = null;
	}

  	private static void load()
  		throws Exception
  	{
  		if ((configPath == null) || (configPath.trim().equals(""))) {
  			throw new LogAnalyzerException("文件为空，请检验");
  		}
  		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
  		DocumentBuilder builder = null;
  		try
  		{
  			builder = factory.newDocumentBuilder();
  		}
  		catch (ParserConfigurationException e)
  		{
  			throw new LogAnalyzerException("解析SqlLdrLogAnalyseTemplet.xml配置模板发生异常");
  		}
  		Document doc = null;

  		//加载日志解析模版
  		String TempletFilePath = "." + File.separator + "conf" + File.separator + 
  			configPath;
  		File file1 = new File(TempletFilePath);
  		try
  		{
  			doc = builder.parse(file1);
  		}
  		catch (Exception e)
  		{
  			throw new LogAnalyzerException("解析SqlLdrLogAnalyseTemplet.xml配置模板发生异常", e);
  		}

    
  		NodeList systemRuleNodeList = doc.getElementsByTagName("system-rule");
  		if (systemRuleNodeList.getLength() >= 1)
  		{
  			String maxFileSize;
  			if (doc.getElementsByTagName("max-file-size").item(0).getFirstChild() == null)
  			{
  				maxFileSize = "";
  			}
  			else
  			{
  				maxFileSize = doc.getElementsByTagName("max-file-size").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("maxFileSize", maxFileSize);
  			String isOracleLog;
  			if (doc.getElementsByTagName("is-oracle-log").item(0).getFirstChild() == null)
  			{
  				isOracleLog = "";
  			}
  			else
  			{
  				isOracleLog = doc.getElementsByTagName("is-oracle-log").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("isOracleLog", isOracleLog);
  			String tableName;
  			if (doc.getElementsByTagName("table-name").item(0).getFirstChild() == null)
  			{
  				tableName = "";
  			}
  			else
  			{
  				tableName = doc.getElementsByTagName("table-name").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("tableName", tableName);
  			String loadSuccCount;
  			if (doc.getElementsByTagName("load-succ-count").item(0).getFirstChild() == null)
  			{
  				loadSuccCount = "";
  			}
  			else
  			{
  				loadSuccCount = doc.getElementsByTagName("load-succ-count").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("loadSuccCount", loadSuccCount);
  			String data;
  			if (doc.getElementsByTagName("data").item(0).getFirstChild() == null)
  			{
  				data = "";
  			}
  			else
  			{
  				data = doc.getElementsByTagName("data").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("data", data);
  			String when;
  			if (doc.getElementsByTagName("when").item(0).getFirstChild() == null)
  			{
  				when = "";
  			}
  			else
  			{
  				when = doc.getElementsByTagName("when").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("when", when);
  			String nullField;
  			if (doc.getElementsByTagName("null-field").item(0).getFirstChild() == null)
  			{
  				nullField = "";
  			}
  			else
  			{
  				nullField = doc.getElementsByTagName("null-field").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("nullField", nullField);
  			String skip;
  			if (doc.getElementsByTagName("skip").item(0).getFirstChild() == null)
  			{
  				skip = "";
  			}
  			else
  			{
  				skip = doc.getElementsByTagName("skip").item(0).getFirstChild().getNodeValue();
  			}

  			configMap.put("skip", skip);
  			String read;
  			if (doc.getElementsByTagName("read").item(0).getFirstChild() == null)
  			{
  				read = "";
  			}
  			else
  			{
  				read = doc.getElementsByTagName("read").item(0).getFirstChild().getNodeValue();
  			}
  			configMap.put("read", read);
  			String refuse;
  			if (doc.getElementsByTagName("refuse").item(0).getFirstChild() == null)
  			{
  				refuse = "";
  			}
  			else
  			{
  				refuse = doc.getElementsByTagName("refuse").item(0).getFirstChild().getNodeValue();
  			}
  			configMap.put("refuse", refuse);
  			String abandon;
  			if (doc.getElementsByTagName("abandon").item(0).getFirstChild() == null)
  			{
  				abandon = "";
  			}
  			else
  			{
  				abandon = doc.getElementsByTagName("abandon").item(0).getFirstChild().getNodeValue();
  			}	
  			configMap.put("abandon", abandon);
  			
  			//SQL LOAD 开始执行时间，结束执行时间
  			String startTime;
  			if (doc.getElementsByTagName("start-time").item(0).getFirstChild() == null)
  			{
  				startTime = "";
  			}
  			else
  			{
  				startTime = doc.getElementsByTagName("start-time").item(0).getFirstChild().getNodeValue();
  			}
  			configMap.put("startTime", startTime);
  			String endTime;
  			if (doc.getElementsByTagName("end-time").item(0).getFirstChild() == null)
  			{
  				endTime = "";
  			}
  			else
  			{
  				endTime = doc.getElementsByTagName("end-time").item(0).getFirstChild().getNodeValue();
  			}
  			configMap.put("endTime", endTime);
  			
  			String runTime;
  			if (doc.getElementsByTagName("run-time").item(0)== null || doc.getElementsByTagName("run-time").item(0).getFirstChild() == null)
  			{
  				runTime = "";
  			}
  			else
  			{
  				runTime = doc.getElementsByTagName("run-time").item(0).getFirstChild().getNodeValue();
  			}
  			configMap.put("runTime", runTime);
  		}

  		//自定义规则，一般是异常信息
  		NodeList userRuleNodeList = doc.getElementsByTagName("user-define-rule");
  		if (userRuleNodeList.getLength() >= 1)
  		{
  			for (int i = 0; i < userRuleNodeList.getLength(); i++)
  			{
  				Node userRule = userRuleNodeList.item(i);
  				NodeList nodelist = userRule.getChildNodes();
  				for (int j = 0; j < nodelist.getLength(); j++)
  				{
  					Node nodeRule = nodelist.item(j);
  					if ((nodeRule.getNodeType() != 1) || 
  							(!nodeRule.getNodeName().toLowerCase().equals("rule")))
  						continue;
  					String rule = getNodeValue(nodeRule);
  					uerDefineRuleList.add(rule);
  				}
  			}
  		}
  	}

  	public void init()
  		throws LogAnalyzerException
    {
    }

  	private static String getNodeValue(Node CurrentNode)
  	{
  		String strValue = "";
  		NodeList nodelist = CurrentNode.getChildNodes();
  		if (nodelist != null)
  		{
  			for (int i = 0; i < nodelist.getLength(); i++)
  			{
  				Node tempnode = nodelist.item(i);
  				if (tempnode.getNodeType() != 3)
  					continue;
  				strValue = tempnode.getNodeValue();
  			}
  		}
  		return strValue;
  	}

  	private String formetFileSize(long fileS)
  	{
  		DecimalFormat df = new DecimalFormat("#.00");
  		String fileSizeString = "";
  		if (fileS < 1024L)
  		{
  			fileSizeString = df.format(fileS) + "B";
  		}
  		else if (fileS < 1048576L)
  		{
  			fileSizeString = df.format(fileS / 1024.0D) + "K";
  		}
  		else if (fileS < 1073741824L)
  		{
  			fileSizeString = df.format(fileS / 1048576.0D) + "M";
  		}
  		else
  		{
  			fileSizeString = df.format(fileS / 1073741824.0D) + "G";
  		}
  		return fileSizeString;
  	}

  	private boolean isOracleLog(InputStream in)
  		throws Exception
    {
  		try
  		{
  			String content = "";
  			byte[] buffer = new byte[100];
  			in.read(buffer, 0, buffer.length);
  			content = new String(buffer);
  			return content.contains((CharSequence)configMap.get("isOracleLog"));
  		}
  		catch (Exception e) {
  			throw e;
  		}
    
    }

  	private SqlldrResult getSqlldrAnalyseResult(String strLineBuffer, boolean isOracleLog, int fileSize)
  	{
  		SqlldrResult sqlldrResult = new SqlldrResult();
  		String maxFileSize = formetFileSize(fileSize);
  		String tableName = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("tableName"), 1);
  		String loadSuccCount = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("loadSuccCount"), 1);

  		String data = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("data"), 1);

  		String when = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("when"), 1);

  		String nullField = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("nullField"), 1);

  		String skip = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("skip"), 1);

  		String read = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("read"), 1);

  		String refuse = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("refuse"), 1);
  		String abandon = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("abandon"), 1);
  		String startTime = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("startTime"), 1);

  		String endTime = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("endTime"), 1);
  		String runTime = regexQueryGroup(strLineBuffer.toString(), (String)configMap.get("runTime"), 1).replace(" ", "");

  		sqlldrResult.setMaxFileSize(maxFileSize);
  		sqlldrResult.setData(stringToInt(data));
  		sqlldrResult.setOracleLog(isOracleLog);
  		sqlldrResult.setTableName(tableName);
  		sqlldrResult.setLoadSuccCount(stringToInt(loadSuccCount));
  		sqlldrResult.setWhen(stringToInt(when));
  		sqlldrResult.setNullField(stringToInt(nullField));
    	sqlldrResult.setSkip(stringToInt(skip));
    	sqlldrResult.setRead(stringToInt(read));

    	sqlldrResult.setRefuse(stringToInt(refuse));
    	sqlldrResult.setAbandon(stringToInt(abandon));
    	sqlldrResult.setStartTime(startTime);
    	sqlldrResult.setEndTime(endTime);
    	
    	
		
		try {
			SimpleDateFormat f = new SimpleDateFormat("HH:mm:ss.SS");
			SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");
			Date dtRunTime = new Date();
			if(!runTime.equals(""))
			   dtRunTime = f.parse(runTime);
			Date s = sf.parse("1970-01-01");
			long length = dtRunTime.getTime() - s.getTime();
			
			BigDecimal b = new BigDecimal((double)length/(double)1000);
			double f1 = b.setScale(2,   BigDecimal.ROUND_HALF_UP).doubleValue();
			sqlldrResult.setRunTime(f1);
			//System.out.print(length);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	Iterator it = uerDefineRuleList.iterator();
    	while (it.hasNext())
    	{
    		String regRule = (String)it.next();
    		if ((regRule == null) || (
    				(regRule != null) && ("".equals(regRule.trim()))))
    		{
    			continue;
    		}

    		regexQueryGroup(strLineBuffer.toString(), regRule, 0);
    	}
    	sqlldrResult.setRuleList(this.matchRuleList);
    	return sqlldrResult;
  	}

  private String regexQueryGroup(String str, String regEx, int group)
  {
    String resultValue = "";
    if ((regEx == null) || ((regEx != null) && ("".equals(regEx.trim())))) return resultValue;
    Pattern p = Pattern.compile(regEx);
    Matcher m = p.matcher(str);

    int count = 0;
    boolean result = m.find();
    if (result)
    {
      count++;
      resultValue = m.group(group);
      if (group == 0)
      {
        if (!this.matchRuleList.contains(resultValue))
        {
          this.matchRuleList.add(resultValue);
        }
      }
    }
    return resultValue;
  }

  private int stringToInt(String str)
  {
    if ((str == null) || ((str != null) && (str.trim().equals("")))) return 0;
    str = str.trim();
    return Integer.valueOf(str).intValue();
  }
}