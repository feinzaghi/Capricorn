package com.turk.parser;

import java.util.ArrayList;
import java.util.regex.Pattern;

import com.turk.distributor.Distribute;

import org.apache.log4j.Logger;

import com.turk.task.CollectObjInfo;
import com.turk.util.LogMgr;

/**
 * 解析抽象类
 * @author Administrator
 *
 */
public abstract class Parser
{
	protected CollectObjInfo collectObjInfo;
	public Distribute distribute;
	protected String fileName = "";
	protected String dsConfigName = null;

	protected Logger log = LogMgr.getInstance().getSystemLogger();
	protected Logger errorlog = LogMgr.getInstance().getErrorLogger();

	public Parser(CollectObjInfo obj)
	{
		this.collectObjInfo = obj;
		this.distribute = new Distribute(obj);
	}

	public Parser()
	{
	}

	public String getFileName()
	{
		return this.fileName;
	}

	public void setFileName(String strFileName)
	{
		this.fileName = strFileName;
	}

	public String getDsConfigName()
	{
		return this.dsConfigName;
	}

	public void setDsConfigName(String dsConfigName)
	{
		this.dsConfigName = dsConfigName;
	}

	public void init(CollectObjInfo obj)
	{
		this.collectObjInfo = obj;
	}

	/**
	 * 解析数据
	 * @return
	 * @throws Exception
	 */
	public abstract boolean parseData()
		throws Exception;

	public CollectObjInfo getCollectObjInfo()
	{
		return this.collectObjInfo;
	}

	public void setCollectObjInfo(CollectObjInfo collectObjInfo)
	{
		this.collectObjInfo = collectObjInfo;
	}

	public Distribute getDistribute()
	{
		return this.distribute;
	}

	public void setDistribute(Distribute distribute)
	{
		this.distribute = distribute;
	}
	
	public String GetDistributeFile()
	{
		return "";
	}
	
	
	protected static String[] split(String linestr, String splitsign, String upsplitsign)
	{
		if ((upsplitsign == null) || (upsplitsign.length() == 0))
			return linestr.split(splitsign);
		String[] upsplits = upsplitsign.split(",");
		if (upsplits.length < 2)
		{
			upsplits = new String[2];
			upsplits[0] = upsplitsign;
			upsplits[1] = upsplitsign;
		}

		ArrayList<String> alist = new ArrayList<String>();
		boolean espeflag = false;
		int espebeginindex = 0;
		boolean beginflag = false;
		int splitbegindex = 0;

		for (int i = 0; i < linestr.length(); i++)
		{
			if (i == linestr.length() - 1)
			{//行的最后
				if (splitsign.equals(linestr.substring(i, i + 1)))
				{
					alist.add(linestr.substring(splitbegindex, i));
					//alist.add("");
					//alist.add("");
				}
				else
				{
					alist.add(linestr.substring(espeflag ? espebeginindex : splitbegindex, i + 1));
				}
			}
			else if ((upsplits[0].equals(linestr.substring(i, i + 1))) && (!espeflag))
			{
				espeflag = true;
				espebeginindex = i + 1;
			}
			else if (espeflag)
			{
				if (!upsplits[1].equals(linestr.substring(i, i + 1))) {
					continue;
				}
				alist.add(linestr.substring(espebeginindex, i));
				espeflag = false;
				i++;
				splitbegindex = i + 1;
			}
			else if ((splitsign.equals(linestr.substring(i, i + 1))) && (!beginflag))
			{
				beginflag = true;
				alist.add(linestr.substring(splitbegindex, i));
				splitbegindex = i + 1;
			} else {
				if ((!splitsign.equals(linestr.substring(i, i + 1))) || (!beginflag))
					continue;
				alist.add(linestr.substring(splitbegindex, i));
				splitbegindex = i + 1;
			}
		}
		String[] rets = (String[])alist.toArray(new String[0]);
		return rets;
	}
	
	protected String removeNoiseSemicolon(String content)
	{
	    String strValue = content.replaceAll(";", " ");
	    strValue = content.replaceAll(",", " ");
	    return strValue;
	}
	
	protected boolean logicEquals(String shortFileName, String fileName)
	{
		if ((!fileName.contains("*")) && (!fileName.contains("?"))) return shortFileName.equals(fileName);

		String s1 = shortFileName.replaceAll("\\.", "");
		String s2 = fileName.replaceAll("\\.", "");
		s1 = s1.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\+", "");
		s2 = s2.replaceAll("\\*", ".*");
		s2 = s2.replaceAll("\\?", ".");

		return Pattern.matches(s2, s1);
	}
	
	public abstract void Stop();
}