package com.turk.parser;

import java.io.FileReader;
import com.turk.task.CollectObjInfo;

/**
 * 空模版解析
 * 不做解析，读取字符串，直接写本地文件
 * @author Administrator
 *
 */
public class EmptyParser extends Parser
{
	private String m_OddString = "";
	
	public EmptyParser()
	{
	}

	public EmptyParser(CollectObjInfo TaskInfo)
	{
		super(TaskInfo);
	}

	public boolean parseData()
   		throws Exception
    {
		FileReader reader = null;
		try
		{
			this.log.debug("空模板-开始解析" + this.fileName);
			reader = new FileReader(this.fileName);
			char[] buff = new char[65536];

			int iLen = 0;
			while ((iLen = reader.read(buff)) > 0)
			{
				BuildData(buff, iLen);
			}

			String strEnd = "\n**FILEEND**";
			BuildData(strEnd.toCharArray(), strEnd.length());
		}
		finally
		{
			try
			{
				if (reader != null) {
					reader.close();
				}
			}
			catch (Exception localException)
			{
			}
		}
		return true;
    }

	public boolean BuildData(char[] chData, int iLen)
	{
		this.m_OddString += new String(chData, 0, iLen);

		boolean bLastCharN = false;
		if (this.m_OddString.charAt(this.m_OddString.length() - 1) == '\n') {
			bLastCharN = true;
		}

		String[] strzRowData = this.m_OddString.split("\n");

		if (strzRowData.length == 0) {
			return true;
		}

		int nRowCount = strzRowData.length - 1;
		this.m_OddString = strzRowData[nRowCount];
    	if (this.m_OddString.equals("**FILEEND**")) {
    		this.m_OddString = "";
    	}

    	if (bLastCharN) {
    		this.m_OddString += "\n";
    	}

    	for (int i = 0; i < nRowCount; i++)
    	{
    		try
    		{
    			if ((strzRowData[i] == null) || (strzRowData[i].trim().equals("")))
    				continue;
    			ParseLineData(strzRowData[i] + "\n");
    		}
    		catch (Exception e)
    		{
    			this.log.error("BuildData", e);
    		}
    	}

    	return true;
	}	

	public void ParseLineData(String strOldRow)
	{
		this.distribute.DistributeData(strOldRow.getBytes(), 0);
	}

  public static void main(String[] args)
  {
  }

@Override
public void Stop() {
	// TODO Auto-generated method stub
	
}
}