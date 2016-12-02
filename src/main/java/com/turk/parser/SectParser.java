package com.turk.parser;


import java.io.FileReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import com.turk.Config.ConstDef;
import com.turk.task.CollectObjInfo;
import com.turk.templet.SectTempletP;

/**
 * 段解析
 * @author Administrator
 *
 */
public class SectParser extends Parser
{
	private String m_OddString = new String();
	private String CommonKeyField = "";

	public SectParser()
	{
	}

	public SectParser(CollectObjInfo collectInfo)
	{
		super(collectInfo);

		SectTempletP templet = (SectTempletP)this.collectObjInfo.getParseTemplet();
		templet.m_strHeadSectSplitSign = ConstDef.ParseFilePath(templet.m_strHeadSectSplitSign, collectInfo.getLastCollectTime());
		templet.m_strTailSectSplitSign = ConstDef.ParseFilePath(templet.m_strTailSectSplitSign, collectInfo.getLastCollectTime());
	}

	/**
	 * 解析
	 */
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

		SectTempletP templet = (SectTempletP)this.collectObjInfo.getParseTemplet();

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
				SectTempletP.SectTemplet secttemp = (SectTempletP.SectTemplet)templet.m_SectTemplet.get(Integer.valueOf(0));

				int nTempIndex = this.m_OddString.indexOf(secttemp.m_strCommonFieldList);
				if ((secttemp.m_strCommonFieldList.trim() != null) && 
						(secttemp.m_strCommonFieldList.trim() != "") && 
						(nTempIndex >= 0) && (nTempIndex < nLastIndex))
				{
					this.CommonKeyField = this.m_OddString.substring(nTempIndex + 
							secttemp.m_strCommonFieldList.length(), nTempIndex + 
							secttemp.m_strCommonFieldList.length() + 4);
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
						if (templet.m_strTailSectSplitSign.equals("$E")) {
							nLastIndex = this.m_OddString.length() - 1;
						}
						nTempIndex = this.m_OddString.indexOf(secttemp.m_strCommonFieldList);
						if ((secttemp.m_strCommonFieldList.trim() != null) && 
								(secttemp.m_strCommonFieldList.trim() != "") && 
								(nTempIndex >= 0) && (nTempIndex < nLastIndex))
						{
							this.CommonKeyField = this.m_OddString.substring(nTempIndex + 
									secttemp.m_strCommonFieldList.length(), nTempIndex + 
									secttemp.m_strCommonFieldList.length() + 4);
						}
						if (nFirstIndex < 0) break; if (nLastIndex >= 0)
						{
							continue;
						}

					}

				}
				break;
		}

		return false;
	}

	private SectTempletP.SectTemplet GetSectTempletType(String strSectInfo)
	{
		SectTempletP templet = (SectTempletP)this.collectObjInfo.getParseTemplet();
		SectTempletP.SectTemplet sectTemp = null;
		boolean isExist = false;
		for (int i = 0; i < templet.m_SectTemplet.size(); i++)
		{
			sectTemp = (SectTempletP.SectTemplet)templet.m_SectTemplet.get(Integer.valueOf(i));
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
						if (strSectInfo.indexOf(strKeyWord[k]) >= 0)
							continue;
						isExist = false;
						break;
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
			SectTempletP.SectTemplet sectTemp = GetSectTempletType(strSectInfo);
			if (sectTemp == null)
			{
				this.log.error(this.collectObjInfo.getTaskID() + 
						": can't find the templet for the section . " + 
						strSectInfo);
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
				strNewBuffer.append(this.CommonKeyField.trim() + 
    	    		sectTemp.m_strNewSplitSign);
			}
			boolean subfiledflag = false;
			int i = 0;
			for (i = 0; i < sectTemp.m_FieldTemplet.size(); i++)
			{
				SectTempletP.FieldTemplet field = (SectTempletP.FieldTemplet)sectTemp.m_FieldTemplet.get(Integer.valueOf(i));
				switch (field.m_nParseType)
				{
					case 1:
						subfiledflag = false;
						String strValue1 = strSectInfo.substring(field.m_nStartPos, field.m_nStartPos + 
								field.m_nDataLength);

						nCurrentPos = field.m_nStartPos + field.m_nDataLength;
						strNewBuffer.append(strValue1.trim() + 
								sectTemp.m_strNewSplitSign);

						break;
					case 2:
						subfiledflag = false;
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
						subfiledflag = false;
						nStartIndex = nCurrentPos;
						nStartIndex = strSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + 
							field.m_strHeadFieldSign.length();
						String strValue3 = strSectInfo.substring(nStartIndex);
						strNewBuffer.append(strValue3.trim() + 
								sectTemp.m_strNewSplitSign);

						break;
					case 4:
						subfiledflag = false;
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
						strNewBuffer.append(strValue2 + 
								sectTemp.m_strNewSplitSign);

						break;
					case 10:
						String columnLine = sectTemp.headLine;

						if ((columnLine != null) && (!columnLine.equals("")))
						{
							String[] columns = columnLine.split(((SectTempletP.FieldTemplet)sectTemp.m_FieldTemplet.get(Integer.valueOf(i))).m_strSubFieldColSplitSign);
							
							Map<?, ?> map = ((SectTempletP.FieldTemplet)sectTemp.m_FieldTemplet.get(Integer.valueOf(i))).m_SubFieldTemplet;
							for (int j = 0; j < columns.length; j++)
							{
								for (int j2 = 0; j2 < map.size(); j2++)
								{
									SectTempletP.FieldTemplet column = (SectTempletP.FieldTemplet)map.get(Integer.valueOf(j2));
									if (!column.m_strFieldName.equals(columns[j]))
										continue;
									column.dataIndex = j;
									break;
								}
							}
						}

						subfiledflag = true;

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
							if (strRow.trim().equals(""))
								continue;
							String strValue = ParseFieldData(strRow, sectTemp, (SectTempletP.FieldTemplet)sectTemp.m_FieldTemplet.get(Integer.valueOf(i)));

							if ((strValue == null) || (strValue.equals("")))
								continue;
							TempBuild.delete(0, TempBuild.length());
							TempBuild.append(strNewBuffer.toString() + strValue +  "\n");

							this.distribute.DistributeData(TempBuild.toString().getBytes(), sectTemp.m_nSectTypeIndex);
						}
					case 5:
					case 6:
					case 7:
					case 8:
					case 9:
				}
			}
			if (!subfiledflag)
			{
				strNewBuffer.append("\n");
				this.distribute.DistributeData(strNewBuffer.toString().getBytes(), sectTemp.m_nSectTypeIndex);
			}
    	}
		catch (Exception e)
		{
			this.log.error("解析段出错:", e);
		}
	}

	private String ParseFieldData(String strSubSectInfo, SectTempletP.SectTemplet sectTemp, SectTempletP.FieldTemplet sectField)
	{
		StringBuffer strNewBuffer = new StringBuffer();
		Map<?, ?> subFieldTemplets = sectField.m_SubFieldTemplet;
		try
		{
			if (sectField.m_bSubSectSplit)
			{
				String[] strColData;
				if (sectField.m_strSubFieldColSplitSign.equals("\\\\|"))
				{
					strColData = strSubSectInfo.split("\\|");
				}
				else
				{
					strColData = strSubSectInfo.split(sectField.m_strSubFieldColSplitSign);
				}
				boolean isNullData = true;

				if ((sectTemp.headLine != null) && (!sectTemp.headLine.equals("")))
				{
					for (int i = 0; i < strColData.length; i++)
					{
						for (int j = 0; j < subFieldTemplets.size(); j++)
						{
							SectTempletP.FieldTemplet field = (SectTempletP.FieldTemplet)subFieldTemplets.get(Integer.valueOf(j));
							if (field.dataIndex != i)
								continue;
							if (strColData[i].trim().equals("")) {
								strNewBuffer.append("0" + 
										sectTemp.m_strNewSplitSign);
								break;
							}

							isNullData = false;
							strNewBuffer.append(strColData[i].trim() + 
									sectTemp.m_strNewSplitSign);

							break;
						}
					}
				}
				else
				{
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
				}

				if (isNullData)
					return "";
			}
			else
			{
				int nCurrentPos = 0;

				for (int i = 0; i < sectField.m_SubFieldTemplet.size(); i++)
				{
					SectTempletP.FieldTemplet field = (SectTempletP.FieldTemplet)sectField.m_SubFieldTemplet.get(Integer.valueOf(i));
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
								strNewBuffer.append("0" + 
										sectTemp.m_strNewSplitSign);
							}
							String strValue2 = "";
							if (nEndIndex < 0)
								strValue2 = strSubSectInfo.substring(nStartIndex);
							else
								strValue2 = strSubSectInfo.substring(nStartIndex, nEndIndex);
							nCurrentPos = nEndIndex;
							strNewBuffer.append(strValue2 + sectTemp.m_strNewSplitSign);

							break;
						case 3:
							nStartIndex = nCurrentPos;
							nStartIndex = strSubSectInfo.indexOf(field.m_strHeadFieldSign, nStartIndex) + 
							field.m_strHeadFieldSign.length();
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
		} catch (Exception Err) {
			Err.printStackTrace();
		}
		return strNewBuffer.toString();
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}