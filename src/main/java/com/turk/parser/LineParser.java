package com.turk.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import com.turk.config.ConstDef;
import com.turk.parser.config.CityConfig;
import com.turk.task.CollectObjInfo;
import com.turk.task.RegatherObjInfo;
import com.turk.task.TaskMgr;
import com.turk.templet.LineTempletP;
import com.turk.util.Util;

/**
 * 行解析
 * @author Turk
 *
 */
public class LineParser extends Parser
{
	private String remainingData = "";

	private int m_ParseTime = 0;

	private String m_RawColumn_List = "";

	public LineParser()
	{
	}

	public LineParser(CollectObjInfo collectInfo)
  	{
		super(collectInfo);
  	}

	public static String[] split(String linestr, String splitsign, String upsplitsign)
	{
		if ((upsplitsign == null) || (upsplitsign.length() == 0))
			return linestr.split(splitsign);
		String[] upsplits = upsplitsign.split(",",-1);
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
			{
				if (splitsign.equals(linestr.substring(i, i + 1)))
				{
					alist.add("");
					alist.add("");
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

	/*
	public static boolean testData(char[] chData, int iLen)
	{
		String oddstr = "";
		oddstr = oddstr + new String(chData, 0, iLen);

		boolean bLastCharN = false;
		if (oddstr.charAt(oddstr.length() - 1) == '\n') {
			bLastCharN = true;
		}

		String[] strzRowData = oddstr.split("\n");

		if (strzRowData.length == 0) {
			return true;
		}

		int nRowCount = strzRowData.length - 1;
		oddstr = strzRowData[nRowCount];
		if (oddstr.equals("**FILEEND**")) {
			oddstr = "";
		}

		if (bLastCharN)
			oddstr = oddstr + "\n";
		FileWriter fw = null;
		try
		{
			fw = new FileWriter("/mrdata/testddd.txt");

			for (int i = 0; i < nRowCount; i++)
			{
				try
				{
					if ((strzRowData[i] == null) || 
							(strzRowData[i].trim().equals("")))
					{
						continue;
					}

					String linstr = AutoCharset.getCorrectString(new String(strzRowData[i].getBytes(), "ISO-8859-1"), new String("is中文=1".getBytes(), "ISO-8859-1"));
					fw.write(linstr + ";\n");
				}
				catch (Exception ex)
				{
					ex.printStackTrace();
				}
			}
			fw.close();
		}
		catch (Exception localException1)
		{
			errorlog.error(localException1);
		}

		return true;
	}*/


	/**
	 * 数据解析
	 */
  	@SuppressWarnings("resource")
	public boolean parseData() throws Exception
  	{
  		//FileReader reader = null;
  		FileInputStream fis = null;
  		try
  		{
  			String logStr = this + ": starting parse file : " + this.fileName;
  			this.log.debug(logStr);
  			this.collectObjInfo.log("解析", logStr);

  			//行解析模版
  			LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
  			
  			File fs = new File(this.fileName);
      
  			fis = new FileInputStream(fs);
      
  			BufferedReader br = null;
  			if(templet.m_strEncode.isEmpty())
  			{
  				br = new BufferedReader(new InputStreamReader(fis));
  			}
  			else
  			{
  				br = new BufferedReader(new InputStreamReader(fis,templet.m_strEncode));
  			}
      
  			char[] buff = new char[65536];

  			
  			int iLen = 0;
  			while ((iLen = br.read(buff)) > 0)
  			{
  				BuildData(buff, iLen);
  			}

  			String strEnd = "\n**FILEEND**";
  			BuildData(strEnd.toCharArray(), strEnd.length());
  		}
  		catch (Exception ex)
		{
  			errorlog.error("LineParser Error",ex);
		}
  		finally
  		{
  			try
  			{
  				if (fis != null) {
  					fis.close();
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
  		boolean bReturn = true;

  		this.remainingData += new String(chData, 0, iLen);

  		String logStr = null;

  		if (++this.m_ParseTime % 100 == 0)
  		{
  			logStr = this + ": " + this.collectObjInfo.getDescribe() + 
  			" parse time:" + this.m_ParseTime;
  			this.log.debug(logStr);
  			this.collectObjInfo.log("解析", logStr);
  		}
  		boolean bLastCharN = false;
  		if (this.remainingData.charAt(this.remainingData.length() - 1) == '\n') {
  			bLastCharN = true;
  		}

  		String[] strzRowData = this.remainingData.split("\n");

  		if (strzRowData.length == 0) {
  			return true;
  		}

  		int nRowCount = strzRowData.length - 1;
  		this.remainingData = strzRowData[nRowCount];
  		if (this.remainingData.equals("**FILEEND**")) {
  			this.remainingData = "";
  		}

  		if (bLastCharN) {
  			this.remainingData += "\n";
  		}

  		try
  		{
  			for (int i = 0; i < nRowCount; i++)
  			{
  				if (Util.isNull(strzRowData[i]))
  					continue;
  				ParseLineData(strzRowData[i]);
  			}
  		}
  		catch (Exception e)
  		{
  			bReturn = false;
  			logStr = this + ": Cause:";
  			this.errorlog.error(logStr, e);
  			this.collectObjInfo.log("解析", logStr, e);
  		}

  		return bReturn;
  	}

  	/**
  	 * 行解析
  	 * @param strOldRow
  	 */
	public void ParseLineData(String strOldRow)
	{
		boolean isExistReservedKeyWord = false;

		int nSubTmpIndex = -1;
		int nColumnIndex = 0;

		//行解析模版
		LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();

		switch (templet.nScanType)
		{
			case 0:
				for (int j = 0; j < templet.unReserved.size(); j++)
				{
					if (strOldRow.indexOf((String)templet.unReserved.get(j)) == 0)
						return;
				}
				break;
			case 1:
				for (int i = 0; i < templet.m_nTemplet.size(); i++)
				{
					LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(i);

					if (strOldRow.indexOf(subTemp.m_strLineHeadSign) != 0)
						continue;
					isExistReservedKeyWord = true;
					nSubTmpIndex = i;
					nColumnIndex = 1;
					break;
				}

				if (isExistReservedKeyWord) break;
					return;
			case 2:
				String strShortFileName = this.fileName.substring(this.fileName.lastIndexOf(File.separatorChar) + 1);
				for (int i = 0; i < templet.m_nTemplet.size(); i++)
				{
					LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(i);

					String strFileName = ConstDef.ParseFilePath(subTemp.m_strFileName, this.collectObjInfo.getLastCollectTime());
					if (subTemp.m_nFileNameCompare == 0)
					{
						if(strFileName.isEmpty())
							continue;
						
						if (!logicEquals(strShortFileName, strFileName))
						{
							if(!strFileName.isEmpty() && !strShortFileName.contains(strFileName))
							{
								continue;
							}
						}
						nSubTmpIndex = i;

						if (!TaskMgr.getInstance().isReAdoptObj(this.collectObjInfo))
							break;
						RegatherObjInfo rTask = (RegatherObjInfo)this.collectObjInfo;
						rTask.addTableIndex(i);

						break;
					}

					if (subTemp.m_nFileNameCompare != 1)
						continue;
					if (strShortFileName.indexOf(strFileName) != 0)
						continue;
					nSubTmpIndex = i;
					this.collectObjInfo.setActiveTableIndex(i);
					break;
				}

				for (int j = 0; j < templet.unReserved.size(); j++)
				{
					if (strOldRow.indexOf((String)templet.unReserved.get(j)) == 0) {
						return;
					}
				}

		}
		
		if(nSubTmpIndex == -1)
			return;

		LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
			.get(nSubTmpIndex);

		StringBuffer strNewRow = new StringBuffer();
		String strValue = "";
		
		//若为表头，不解析，跳过
		if (!subTemp.m_strLineHeadSign.isEmpty() && strOldRow.indexOf(subTemp.m_strLineHeadSign) > 0)
			return;

		switch(subTemp.m_nDefaultColumnType)
		{
			case 0:
				break;
			case 1:
				//加入解析头固定时间等字符串,DEVICEID,COLLECTTIME,STAMPTIME;
				if ((subTemp.m_RawColumnList != null) && 
						(!subTemp.m_RawColumnList.equals("")) && 
						(strOldRow.indexOf(subTemp.m_RawColumnList) == 0))
				{
					if (subTemp.m_ColumnListAppend != "")
						this.m_RawColumn_List = 
							(subTemp.m_ColumnListAppend + 
									subTemp.m_strNewFieldSplitSign + strOldRow);
					strOldRow = this.m_RawColumn_List;
					strNewRow.append("DEVICEID" + subTemp.m_strNewFieldSplitSign + 
							"COLLECTTIME" + subTemp.m_strNewFieldSplitSign + 
							"STAMPTIME" + subTemp.m_strNewFieldSplitSign);
				}
				else
				{
					strNewRow.append(this.collectObjInfo.getDevInfo().getDevID());
					strNewRow.append(subTemp.m_strNewFieldSplitSign);

					Date now = new Date();
					SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String strTime = spformat.format(now);
					strNewRow.append(strTime + subTemp.m_strNewFieldSplitSign);

					strTime = spformat.format(this.collectObjInfo.getLastCollectTime());

					strNewRow.append(strTime + subTemp.m_strNewFieldSplitSign);
				}
				break;
			case 2:
				//加入解析头固定时间等字符串
				if ((subTemp.m_RawColumnList != null) && 
						(!subTemp.m_RawColumnList.equals("")) && 
						(strOldRow.indexOf(subTemp.m_RawColumnList) == 0))
				{
					if (subTemp.m_ColumnListAppend != "")
						this.m_RawColumn_List = 
							(subTemp.m_ColumnListAppend + 
									subTemp.m_strNewFieldSplitSign + strOldRow);
					strOldRow = this.m_RawColumn_List;
					strNewRow.append(
							"START_TIME" + subTemp.m_strNewFieldSplitSign);
				}
				else
				{
					SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String strTime = spformat.format(this.collectObjInfo.getLastCollectTime());
					strNewRow.append(strTime + subTemp.m_strNewFieldSplitSign);
				}
				break;
			case 3:
				//加入解析头固定时间+地市编号 字段
				if ((subTemp.m_RawColumnList != null) && 
						(!subTemp.m_RawColumnList.equals("")) && 
						(strOldRow.indexOf(subTemp.m_RawColumnList) == 0))
				{
					if (subTemp.m_ColumnListAppend != "")
						this.m_RawColumn_List = 
							(subTemp.m_ColumnListAppend + 
									subTemp.m_strNewFieldSplitSign + strOldRow);
					strOldRow = this.m_RawColumn_List;
					strNewRow.append(
							"START_TIME" + subTemp.m_strNewFieldSplitSign);
					strNewRow.append(
							"CITY_ID" + subTemp.m_strNewFieldSplitSign);
				}
				else
				{
					SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String strTime = spformat.format(this.collectObjInfo.getLastCollectTime());
					strNewRow.append(strTime + subTemp.m_strNewFieldSplitSign);
					
					int nCityID = this.collectObjInfo.getDevInfo().getCityID();
					switch(this.collectObjInfo.getDevInfo().getCityID())
					{//对应不同的获取地市编号的方法
						case 1001:
							try{//ZTE CM
								String strShortFileName = this.fileName.substring(this.fileName.lastIndexOf(File.separatorChar) + 1);
								strShortFileName = strShortFileName.substring(3,5);
								nCityID = CityConfig.getInstance().getCityIDbyEnname(strShortFileName);
							}catch(Exception ex)
							{
								log.error("CM-ZTE,get cityid error",ex);
							}
							break;
						default:
							nCityID = this.collectObjInfo.getDevInfo().getCityID();
					}
					
					
					strNewRow.append(nCityID
							+ subTemp.m_strNewFieldSplitSign);
				}
				break;
		}
		

		try
		{
			switch (subTemp.m_nParseType)
			{
				case 1:
					strValue = ParseRowBySplit(subTemp, nColumnIndex, strOldRow);
					break;
				case 2:
					strValue = ParseRowByPosition(subTemp, strOldRow);
					break;
				case 3:
					strValue = ParsrRowByRaw(subTemp, strOldRow);
					break;
				case 4:
					strValue = strOldRow.replace(subTemp.m_strFieldSplitSign, subTemp.m_strNewFieldSplitSign);
					if (!strValue.endsWith(subTemp.m_strNewFieldSplitSign)) 
						break;
					strValue = strValue.concat(subTemp.m_strNewFieldSplitSign);
			}

		}
		catch (Exception e)
		{
			String str = this + " : error when parsing data. templet name : " + 
				templet.tmpName + " data:" + strOldRow;
			this.log.error(str, e);
			this.collectObjInfo.log("解析", str, e);
			return;
		}

		strNewRow.append(strValue);
		if(this.distribute.getDisTemplet().stockStyle == 4)
		{
			//以文件方式入库  需要把最后一个逗号去掉
			strNewRow.deleteCharAt(strNewRow.length() -1);
		}
		strNewRow.append("\n");
		if(this.distribute.getDisTemplet().encode.isEmpty())
		{
			this.distribute.DistributeData(strNewRow.toString().getBytes(), nSubTmpIndex);
		}
		else
		{//需要对文件字符编码做转换
			try {
				this.distribute.DistributeData(strNewRow.toString().getBytes(this.distribute.getDisTemplet().encode), nSubTmpIndex);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				errorlog.error("分发文件编码格式转换异常",e);
			}
		}
	}


	private String ParsrRowByRaw(LineTempletP.SubTemplet subTemp, String strRow)
	{
		String[] m_strTemp = strRow.split(subTemp.m_strFieldSplitSign,-1);
		strRow = strRow.replaceAll(subTemp.m_strFieldSplitSign, subTemp.m_strNewFieldSplitSign);
		if (m_strTemp.length < subTemp.m_nColumnCount)
		{
			int nCount = subTemp.m_nColumnCount - m_strTemp.length;
			for (int i = 0; i < nCount; i++)
				strRow = strRow + subTemp.m_strNewFieldSplitSign;
		}
		return strRow;
	}

	private String ParseRowBySplit(LineTempletP.SubTemplet subTemp, int nColumnIndex, String strRow)
	{
		LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
		String[] m_strTemp;
		if ((subTemp.m_strFieldUpSplitSign == null) || 
				(subTemp.m_strFieldUpSplitSign.length() == 0))
			m_strTemp = strRow.split(subTemp.m_strFieldSplitSign,-1);
		else {
			m_strTemp = split(strRow, subTemp.m_strFieldSplitSign, subTemp.m_strFieldUpSplitSign);
		}

		StringBuffer m_TempString = new StringBuffer();

		int nCount = 0;
		String nvl = subTemp.nvl;
		for (int k = nColumnIndex; k < m_strTemp.length; k++)
		{
			if ((templet.columnMapping.size() > 0) && 
					(!templet.columnMapping.containsKey(Integer.valueOf(k))))
			{
				continue;
			}
			if (nCount >= subTemp.m_nColumnCount)
			{
				break;
			}
			if ((m_strTemp[k] == null) || (m_strTemp[k].trim().equals("")))
			{
				m_TempString.append(nvl);
			}
			else
			{
				try
				{
					String type = ((LineTempletP.FieldTemplet)subTemp.m_Filed.get(Integer.valueOf(k+3))).m_type;
					if ((type != null) && (type.equals("DATE")))
					{
						String dateFormat = ((LineTempletP.FieldTemplet)subTemp.m_Filed.get(Integer.valueOf(k+3))).m_dateFormat;
						SimpleDateFormat format1 = new SimpleDateFormat(dateFormat);
						SimpleDateFormat format2 = new SimpleDateFormat(dateFormat);

						Date date = format1.parse(m_strTemp[k].trim());
						String resultDate = format2.format(date);
						m_TempString.append(resultDate);
					}
					else
					{
						m_TempString.append(removeNoiseSemicolon(m_strTemp[k].trim()));
					}
				}
				catch (ParseException e)
				{
					m_TempString.append("1970-1-1 08:00:00");
				}
				catch (Exception localException)
				{
					m_TempString.append(removeNoiseSemicolon(m_strTemp[k].trim()));
				}
			}

			//if ((k < m_strTemp.length - 1) && (nCount < subTemp.m_nColumnCount - 1))
			m_TempString.append(subTemp.m_strNewFieldSplitSign);
			nCount++;
		}
		
		if(subTemp.m_nDefaultColumnType == 1)
		{
			if (nCount < subTemp.m_nColumnCount - 3)  //排除默认加入的DEVICEID,COLLECTTIME,STAMPTIME字段
			{
				for (int k = nCount; k < subTemp.m_nColumnCount; k++)
				{
					m_TempString.append(subTemp.m_strNewFieldSplitSign + nvl);
	
					nCount++;
				}
			}
		}
		return m_TempString.toString();
	}

	private String ParseRowByPosition(LineTempletP.SubTemplet subTemp, String strRow)
	{
	    StringBuffer m_TempString = new StringBuffer();
	    int len = subTemp.m_Filed.size();
	    String nvl = subTemp.nvl;
	    for (int i = 0; i < len; i++)
	    {
	    	LineTempletP.FieldTemplet field = (LineTempletP.FieldTemplet)subTemp.m_Filed.get(Integer.valueOf(i));

	      String strValue = "";
	      if (field.m_nStartPos + field.m_nDataLength > strRow.length())
	    	  strValue = strRow.substring(field.m_nStartPos);
	      else {
	    	  strValue = strRow.substring(field.m_nStartPos, field.m_nStartPos + 
	    			  field.m_nDataLength);
	      }
      if (i < subTemp.m_Filed.size() - 1)
      {
        if (strValue.trim().equals(""))
        {
          m_TempString.append(nvl + subTemp.m_strNewFieldSplitSign);
        }
        else
        {
          m_TempString.append(removeNoiseSemicolon(strValue.trim()) + 
            subTemp.m_strNewFieldSplitSign);
        }

      }
      else if (strValue.trim().equals(""))
      {
        m_TempString.append(nvl);
      }
      else
      {
        m_TempString.append(removeNoiseSemicolon(strValue.trim()));
      }

    }

    return m_TempString.toString();
  }


	  public String toString()
	  {
	    String strTaskID = "Line-Parser-" + (
	      this.collectObjInfo == null ? "NULL" : Integer.valueOf(this.collectObjInfo.getTaskID()));
	    return strTaskID;
	  }

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}