package com.turk.parser;


import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.turk.Config.ConstDef;
import com.turk.task.CollectObjInfo;
import com.turk.templet.Sect21TempletP;

/**
 * ¶Î½âÎö
 * @author Administrator
 *
 */
public class Sect21Parser extends Parser
{
	private String m_OddString = new String();

	private String CommonKeyField = "";

	public Sect21Parser()
	{
	}

	public Sect21Parser(CollectObjInfo collectInfo)
	{
	    super(collectInfo);
	
	    Sect21TempletP templet = (Sect21TempletP)this.collectObjInfo.getParseTemplet();
	    templet.m_strHeadSectSplitSign = ConstDef.ParseFilePath(templet.m_strHeadSectSplitSign, collectInfo.getLastCollectTime());
	    templet.m_strTailSectSplitSign = ConstDef.ParseFilePath(templet.m_strTailSectSplitSign, collectInfo.getLastCollectTime());
	}

	public boolean parseData() throws Exception
	{
		FileReader reader = null;
		try
		{
			reader = new FileReader(this.fileName);
			char[] buff = new char[65536];
			StringBuffer sb = new StringBuffer();

			int iLen = 0;
			while ((iLen = reader.read(buff)) > 0)
			{
				sb.append(new String(buff, 0, iLen));
			}

			sb.append("\n**FILEEND**\n");
			BuildData(sb.toString().toCharArray(), sb.length());
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

	public boolean BuildData(char[] byData, int nbyLength)
	{
	    this.m_OddString += new String(byData, 0, nbyLength);
	
	    Sect21TempletP templet = (Sect21TempletP)this.collectObjInfo.getParseTemplet();
	
	    this.CommonKeyField = "";
	    for (int i = 0; i < templet.m_CommonTemplet.size(); i++)
	    {
	    	Sect21TempletP.FieldTemplet field = (Sect21TempletP.FieldTemplet)templet.m_CommonTemplet.get(Integer.valueOf(i));
	    	int iBegin = this.m_OddString.indexOf(field.m_strHeadFieldSign) + 
	    		field.m_strHeadFieldSign.length();
	    	int iEnd = this.m_OddString.indexOf(field.m_strTailFieldSign);
	    	this.CommonKeyField = 
	    		(this.CommonKeyField + 
	    				this.m_OddString.substring(iBegin, iEnd) + 
	    				templet.m_strAllNewSplitSign);
	    }

	    switch (templet.m_nSectScanType)
	    {
    	case 1:
    		String[] m_SectData = this.m_OddString.split("\n\n");
    		if (m_SectData.length > 1)
    			this.m_OddString = m_SectData[(m_SectData.length - 1)];
    		for (int i = 0; i < m_SectData.length; i++)
    		{
    			ParseSectData(m_SectData[i].toString().trim());
    		}
    		break;
    	case 2:
    		int nFirstIndex = this.m_OddString.indexOf(templet.m_strHeadSectSplitSign);

    		if (templet.m_strHeadSectSplitSign.equals("$S"))
    			nFirstIndex = 0;
    		int nLastIndex = this.m_OddString.indexOf(templet.m_strTailSectSplitSign);

    		if (templet.m_strTailSectSplitSign.equals("$E")) {
    			nLastIndex = this.m_OddString.length() - 1;
    		}

    		String strSectInfo = "";
    		while (true)
    		{
    			strSectInfo = "";
    			strSectInfo = this.m_OddString.substring(nFirstIndex, nLastIndex);
    			ParseSectData(strSectInfo.trim());

    			if (nLastIndex + templet.m_strTailSectSplitSign.length() > this.m_OddString.length())
    			{
    				this.m_OddString = "";
    			}
    			else {
    				this.m_OddString = this.m_OddString.substring(nLastIndex + 
    						templet.m_strTailSectSplitSign.length());

    				nFirstIndex = this.m_OddString.indexOf(templet.m_strHeadSectSplitSign);
    				if (templet.m_strHeadSectSplitSign.equals("$S"))
    					nFirstIndex = 0;
    				nLastIndex = this.m_OddString.indexOf(templet.m_strTailSectSplitSign);
    				if (templet.m_strTailSectSplitSign.equals("$E"))
    					nLastIndex = this.m_OddString.length() - 1;
    				if (nFirstIndex < 0) break; if (nLastIndex >= 0)
    				{
    					continue;
    				}
    			}
    		}
	    }
	    return false;
	}

	private Sect21TempletP.SectTemplet GetSectTempletType(String strSectInfo)
	{
	    Sect21TempletP templet = (Sect21TempletP)this.collectObjInfo.getParseTemplet();
	    Sect21TempletP.SectTemplet sectTemp = null;
	    boolean isExist = false;
	    for (int i = 0; i < templet.m_SectTemplet.size(); i++)
	    {
	    	sectTemp = (Sect21TempletP.SectTemplet)templet.m_SectTemplet.get(Integer.valueOf(i));
	    	switch (sectTemp.m_nSectKeySearchType)
	    	{
	    	case 1:
	    		if (strSectInfo.indexOf(sectTemp.m_strSectKeyWord) != 0) break;
	    		isExist = true;
	    		break;
	    	case 2:
	    		if (strSectInfo.lastIndexOf(sectTemp.m_strSectKeyWord) != strSectInfo.length() - sectTemp.m_strSectKeyWord.length()) break;
	    		isExist = true;
	    		break;
	    	case 3:
	    		String[] strKeyWord = sectTemp.m_strSectKeyWord.split(";");
	    		isExist = true;
	    		for (int k = 0; k < strKeyWord.length; k++)
	    		{
	    			if (strSectInfo.indexOf(strKeyWord[k]) < 0) {
	    				isExist = false;
	    			}
	    		}
	    	}
	    	if (isExist) {
	    		break;
	    	}
	    	sectTemp = null;
	    }
	    return sectTemp;
	}

	private void ParseSectData(String strSectInfo)
	{
		try
		{
			Sect21TempletP.SectTemplet sectTemp = GetSectTempletType(strSectInfo);

			if (sectTemp == null) {
				return;
			}
			StringBuffer strNewBuffer = new StringBuffer();
			strNewBuffer.append(this.collectObjInfo.getDevInfo().getDevID());
			strNewBuffer.append(sectTemp.m_strNewSplitSign);

			Date now = new Date();
			SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String strTime = spformat.format(now);
			strNewBuffer.append(strTime + sectTemp.m_strNewSplitSign);

			strTime = spformat.format(this.collectObjInfo.getLastCollectTime());

			strNewBuffer.append(strTime + sectTemp.m_strNewSplitSign);

			int nCurrentPos = 0;

			if ((this.CommonKeyField != null) && (this.CommonKeyField.trim() != "")) {
				strNewBuffer.append(this.CommonKeyField.trim());
			}
			for (int i = 0; i < sectTemp.m_FieldTemplet.size(); i++)
			{
				Sect21TempletP.FieldTemplet field = (Sect21TempletP.FieldTemplet)sectTemp.m_FieldTemplet.get(Integer.valueOf(i));
				switch (field.m_nParseType)
				{
					case 1:
						String strValue1 = strSectInfo.substring(field.m_nStartPos, field.m_nStartPos + 
								field.m_nDataLength);
						nCurrentPos = field.m_nStartPos + field.m_nDataLength;
						strNewBuffer.append(strValue1.trim() + 
								sectTemp.m_strNewSplitSign);
						break;
					case 2:
						int nStartIndex = nCurrentPos;
						int nEndIndex = nStartIndex + 
						field.m_strHeadFieldSign.length();

						if (field.m_strHeadFieldSign.equals("$S")) {
							nStartIndex = 0;
						}
						else if (strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) >= 0) {
							nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + 
							field.m_strHeadFieldSign.length();
						}
						else {
							strNewBuffer.append(
									sectTemp.m_strNewSplitSign);
							continue;
						}
						
						if (field.m_strHeadFieldSign.equals("$E"))
							nEndIndex = strSectInfo.length() - 1;
						else {
							nEndIndex = strSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);
						}
						if (nStartIndex < 0)
						{
							strNewBuffer.append(sectTemp.m_strNewSplitSign);
						}
						String strValue2 = "";
						if (nEndIndex < 0)
							strValue2 = strSectInfo.substring(nStartIndex);
						else
							strValue2 = strSectInfo.substring(nStartIndex, nEndIndex);
						strValue2 = strValue2.trim();
						if (strValue2.indexOf('\n') >= 0) {
							strValue2 = strValue2.replace('\n', '|');
						}
						nCurrentPos = nEndIndex;
						strNewBuffer.append(strValue2 + 
								sectTemp.m_strNewSplitSign);

						break;
					case 3:
						nStartIndex = nCurrentPos;
						nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + 
							field.m_strHeadFieldSign.length();
						String strValue3 = strSectInfo.substring(nStartIndex);
						strNewBuffer.append(strValue3.trim() + 
								sectTemp.m_strNewSplitSign);
						break;
					case 4:
						nStartIndex = 0;
						nEndIndex = 0 + field.m_strHeadFieldSign.length();
						if (field.m_strHeadFieldSign.equals("$S")) {
							nStartIndex = 0;
						}
						else if (strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) >= 0) {
							nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + 
							field.m_strHeadFieldSign.length();
						}
						else {
							strNewBuffer.append(
									sectTemp.m_strNewSplitSign);
							continue;
						}
						if (field.m_strHeadFieldSign.equals("$E"))
							nEndIndex = strSectInfo.length() - 1;
						else {
							nEndIndex = strSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);
						}
						if (nStartIndex < 0)
						{
							strNewBuffer.append(sectTemp.m_strNewSplitSign);
						}
						strValue2 = "";
						if (nEndIndex < 0)
							strValue2 = strSectInfo.substring(nStartIndex);
						else {
							strValue2 = strSectInfo.substring(nStartIndex, nEndIndex);
						}
						strValue2 = strValue2.trim();
						if (strValue2.indexOf('\n') >= 0) {
							strValue2 = strValue2.replace('\n', '|');
						}
						nCurrentPos = nEndIndex;
						strNewBuffer.append(strValue2 + sectTemp.m_strNewSplitSign);
						break;
					case 10:
						nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign);
						nStartIndex += field.m_strHeadFieldSign.length();

						nEndIndex = strSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);

						if ((nEndIndex < 0) && 
								(field.m_strTailFieldSign.equals("$E"))) {
							nEndIndex = strSectInfo.length();
						}

						String strTemp = strSectInfo.substring(nStartIndex, nEndIndex);

						String[] strRows = strTemp.split(field.m_strSubFieldRowSplitSign);

						StringBuffer TempBuild = new StringBuffer();
						for (int k = 0; k < strRows.length; k++)
						{
							String strRow = strRows[k].trim();
							if (strRow.trim().equals("")) {
								continue;
							}
							String strValue = ParseFieldData(strRow, sectTemp, (Sect21TempletP.FieldTemplet)sectTemp.m_FieldTemplet.get(Integer.valueOf(i)));

							if ((strValue == null) || (strValue.equals("")))
								continue;
							TempBuild.delete(0, TempBuild.length());
							TempBuild.append(strNewBuffer.toString() + strValue + "\n");
						}
						strNewBuffer = TempBuild;
						break;
					case 11:
						nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nCurrentPos);
						nStartIndex += field.m_strHeadFieldSign.length();

						nEndIndex = strSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);

						if ((nEndIndex < 0) && 
								(field.m_strTailFieldSign.equals("$E"))) {
							nEndIndex = strSectInfo.length();
						}

						strTemp = strSectInfo.substring(nStartIndex, nEndIndex);

						String[] strFields = strTemp.split(field.m_strSubFieldColSplitSign);
						int j = 0;
						if ((strSectInfo.indexOf(field.m_strHeadFieldSign, nCurrentPos) >= 0) && 
								(nCurrentPos > 0))
						{
							nCurrentPos = nEndIndex;
							for (; j < strFields.length; j++)
							{
								strNewBuffer.append(strFields[j] + sectTemp.m_strNewSplitSign);
								if (j == field.m_SubFieldTemplet.size() - 1) {
									break;
								}
							}
						}
						if (j == field.m_SubFieldTemplet.size())
							continue;
						for (; j < field.m_SubFieldTemplet.size(); j++)
							strNewBuffer.append(sectTemp.m_strNewSplitSign); 
					case 5:
					case 6:
					case 7:
					case 8:
					case 9:
				}
			}
			strNewBuffer.append('\n');
			this.distribute.DistributeData(strNewBuffer.toString().getBytes(), sectTemp.m_nSectTypeIndex);
		}
		catch (Exception e)
		{
			this.log.error("½âÎö¶Î³ö´í.", e);
		}
	}

	private String ParseFieldData(String strSubSectInfo, Sect21TempletP.SectTemplet sectTemp, Sect21TempletP.FieldTemplet sectField)
	{
		StringBuffer strNewBuffer = new StringBuffer();
		try
		{
			if (sectField.m_bSubSectSplit)
			{
				String[] strColData;
				if (sectField.m_strSubFieldColSplitSign.equals("\\\\|"))
					strColData = strSubSectInfo.split("\\|");
				else
					strColData = strSubSectInfo.split(sectField.m_strSubFieldColSplitSign);
				boolean isNullData = true;
				for (int i = 0; i < strColData.length; i++)
				{
					if (!sectField.m_SubFieldTemplet.containsKey(Integer.valueOf(i)))
						continue;
					if (strColData[i].trim().equals("")) {
						strNewBuffer.append("0" + 
								sectTemp.m_strNewSplitSign);
					}
					else {
						isNullData = false;
						strNewBuffer.append(strColData[i].trim() + 
								sectTemp.m_strNewSplitSign);
					}
				}

				if (isNullData)
					return "";
			}
			else
			{
				int nCurrentPos = 0;
				for (int i = 0; i < sectField.m_SubFieldTemplet.size(); i++)
				{
					Sect21TempletP.FieldTemplet field = (Sect21TempletP.FieldTemplet)sectField.m_SubFieldTemplet.get(Integer.valueOf(i));
					switch (field.m_nParseType)
					{
						case 1:
							String strValue1 = strSubSectInfo.substring(field.m_nStartPos, field.m_nStartPos + 
									field.m_nDataLength);
							nCurrentPos = field.m_nStartPos + field.m_nDataLength;
							strNewBuffer.append(strValue1 + sectTemp.m_strNewSplitSign);
							break;
						case 2:
							int nStartIndex = nCurrentPos;
							int nEndIndex = nStartIndex + field.m_strHeadFieldSign.length();
							nStartIndex = strSubSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + 
								field.m_strHeadFieldSign.length();
							nEndIndex = strSubSectInfo.indexOf(field.m_strTailFieldSign, nStartIndex + 1);

							if (nStartIndex < 0)
							{
								strNewBuffer.append("0" + sectTemp.m_strNewSplitSign);
							}
							String strValue2 = "";
							if (nEndIndex < 0)
							{
								strValue2 = strSubSectInfo.substring(nStartIndex);
								nCurrentPos = strSubSectInfo.length();
							}
							else
							{
								strValue2 = strSubSectInfo.substring(nStartIndex, nEndIndex);
								nCurrentPos = nEndIndex;
							}
							strNewBuffer.append(strValue2 + sectTemp.m_strNewSplitSign);
							break;
						case 3:
							nStartIndex = nCurrentPos;
							nStartIndex = strSubSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + field.m_strHeadFieldSign.length();
							String strValue3 = strSubSectInfo.substring(nStartIndex);
							strNewBuffer.append(strValue3 + sectTemp.m_strNewSplitSign);
							break;
						case 10:
							strNewBuffer.append(ParseFieldData(strSubSectInfo, sectTemp, sectField));
						case 4:
						case 5:
						case 6:
						case 7:
						case 8:
						case 9:
					}
				}
			}
		} catch (Exception e) {
			this.log.error(this.collectObjInfo.getTaskID() + " : ½âÎö¶Î³ö´í", e);
		}

		int iLen = strNewBuffer.length();
		strNewBuffer.delete(iLen - 1, iLen);

		return strNewBuffer.toString();
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}