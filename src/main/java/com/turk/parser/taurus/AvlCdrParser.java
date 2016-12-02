package com.turk.parser.taurus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.parser.Parser;
import com.turk.collect.FTPTool;
import com.turk.config.ConstDef;
import com.turk.config.SystemConfig;
import com.turk.distributor.DistributeTemplet;
import com.turk.distributor.DistributeTemplet.TableTemplet;
import com.turk.templet.LineTempletP;
import com.turk.util.LogMgr;
import com.turk.util.Util;

/**
 * traurus ��Ŀ �������� AVL��ʽ
 * @author Administrator
 *
 */

public class AvlCdrParser extends Parser{
	
	//private Logger logger = LogMgr.getInstance().getSystemLogger();
	String remainingData = "";
	
	private Logger applog = LogMgr.getInstance().getAppLogger("taurus");
	
	/**
	 * �ַ�ģ����
	 */
	int _DisTableID = -1;
	
	int _CityID = 0;
	
	private MonitorHit _monitor;
	
	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		applog.debug(this.collectObjInfo.getTaskID() + 
				":Start parser AVL file��" + this.fileName);

		Date curTime = new Date(this.collectObjInfo.getLastCollectTime().getTime());
		Calendar cal = Calendar.getInstance();
		cal.setTime(curTime);
		int minutes = cal.get(12);
  		int hours = cal.get(11);
		if(hours == 3 && minutes == 0 && this.fileName.contains("A1CC")
				&& !MapImsiMsisdn.getInstance().Loading)
		{//3��0�ֵ��ļ�
			MapImsiMsisdn.getInstance().Clear();
			MapModCell.getInstance().Clear();
			MapNE2City.getInstance().Clear();
			MapLac2City.getInstance().Clear();
		}
		
		if(minutes%10 == 0)
		{//10���Ӷ�һ������
			//
			//MonitorMobileConfig.getInstance().Clear();
		}
		
		if(hours == 1 && minutes == 0)
		{	//�ϴ�ǰһ���index�ļ�
			UploadIndexFile();
		}
		
	    
  		FileInputStream fis = null;
  		try
  		{
  			String logStr = this + ": starting parse file : " + this.fileName;
  			applog.debug(logStr);
  			this.collectObjInfo.log("����", logStr);

  			File fs = new File(this.fileName);
      
  			if (!fs.exists())
  		    {
  		    	this.log.error(this.collectObjInfo.getTaskID() + ":File does not exist��" + this.fileName);
  		    	return false;
  		    }
  			//_monitor = new MonitorHit(fs.getName());
  			
  			fis = new FileInputStream(fs);
      
  			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
      
  			char[] buff = new char[65536];

  			int iLen = 0;
  			while ((iLen = br.read(buff)) > 0)
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
  				if (fis != null) {
  					fis.close();
  				}
  			}
  			catch (Exception localException)
  			{
  			}
  		}
  		
  		//_monitor.CommitTouchHit();
  		
		return true;
	}
	
	public boolean BuildData(char[] chData, int iLen)
  	{
  		boolean bReturn = true;

  		this.remainingData += new String(chData, 0, iLen);

  		String logStr = null;

  		
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
  			applog.error(logStr, e);
  			this.collectObjInfo.log("����", logStr, e);
  		}

  		return bReturn;
  	}
	
	/**
  	 * �н���
  	 * @param strOldRow
  	 */
	public void ParseLineData(String strOldRow)
	{

		int nSubTmpIndex = 0;

		//�н���ģ��
		LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();

		switch (templet.nScanType)
		{
			case 0:
			case 1:
				applog.error("�н���ģ��ɨ���������ô��󣬴˴�Ӧ��Ϊ [2]");
				break;
			case 2:
			case 3:
				String strShortFileName = this.fileName.substring(this.fileName.lastIndexOf(File.separatorChar) + 1);
				for (int i = 0; i < templet.m_nTemplet.size(); i++)
				{
					LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(i);

					String strFileName = ConstDef.ParseFilePath(subTemp.m_strFileName, this.collectObjInfo.getLastCollectTime());
					if (subTemp.m_nFileNameCompare == 0)
					{
						if (!logicEquals(strShortFileName, strFileName))
							continue;
						nSubTmpIndex = i;
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
				//����µ����󣬸����ֶε����������ֵ��У�ͬһ�������ļ�Ҫ�ֳɶ���ļ��ַ�
				//�����ֶΣ�ѡ�����Ӧ�ķַ��ļ�ID
				break;

		}

		LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
			.get(nSubTmpIndex);

		StringBuffer strNewRow = new StringBuffer();
		String strValue = "";

		try
		{
			switch (subTemp.m_nParseType)
			{
				case 1:
				case 2:
				case 3:
				case 4:
					applog.warn("ParseType ���ô��󣬴˴�ӦΪ 5 ");
					return;
				case 5://AVL����
					strValue = ParseRowBySplit(subTemp, strOldRow);
					if(templet.nScanType == 3)
					{
						if(_DisTableID == -10000)
							return;
						//����ĳ���ֶ��������ַ����ļ����ĸ�(�ֵ������)
						nSubTmpIndex = _DisTableID;
					}
					break;
			}

		}
		catch (Exception e)
		{
			String str = this.collectObjInfo.getTaskID() + " : error when parsing data. templet name : " + 
				templet.tmpName + " data:" + strOldRow;
			applog.error(str, e);
			this.collectObjInfo.log("����", str, e);
			return;
		}
		strNewRow.append(strValue);
		strNewRow.deleteCharAt(strNewRow.length()-1);//ȥ�����һ������
		strNewRow.append("\n");
		try {
			this.distribute.DistributeData(strNewRow.toString().getBytes("UTF-8"), nSubTmpIndex);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			applog.error("ת��UTF-8��ʽ�쳣",e);
		}
	}
	
	
	private String ParseRowBySplit(LineTempletP.SubTemplet subTemp, String strRow)
	{
		LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
		StringBuffer m_TempString = new StringBuffer();
		String[] m_strTemp;
		if ((subTemp.m_strFieldUpSplitSign == null) || 
				(subTemp.m_strFieldUpSplitSign.length() == 0))
			m_strTemp = strRow.split(subTemp.m_strFieldSplitSign);
		else {
			m_strTemp = split(strRow, subTemp.m_strFieldSplitSign, subTemp.m_strFieldUpSplitSign);
		}
		
		int nCount = 0;
		String nvl = subTemp.nvl;
		Map<String,String> kv = new HashMap<String, String>();
		for (int k = 0; k < m_strTemp.length; k++)
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
				kv.put(subTemp.m_Filed.get(k).m_strFieldName, nvl);
			}
			else
			{
				try
				{
					//����ֵ�����ֵ��
					kv.put(subTemp.m_Filed.get(k).m_strFieldName, removeNoiseSemicolon(m_strTemp[k].trim()));
				}
				catch (Exception ex)
				{
					applog.error("���������ݼ����ֵ��ʱ����",ex);
					//kv.put(subTemp.m_Filed.get(k).m_strFieldName, removeNoiseSemicolon(m_strTemp[k].trim()));
				}
			}
			nCount++;
		}
		if(templet.nScanType == 3)
		{
			_DisTableID = GetDisTempTableID(subTemp.m_tag,kv);
			if(_DisTableID == -10000)
				return "";
		}
		
		TaurusParser parser = new TaurusParser(_monitor,this);
		if(subTemp.m_tag.equals("CC"))
		{
			m_TempString.append(parser.parserCC(subTemp,kv));
		}
		else if(subTemp.m_tag.equals("SM"))
		{
			m_TempString.append(parser.parserSM(subTemp,kv));
		}
		else if(subTemp.m_tag.equals("MM"))
		{
			m_TempString.append(parser.parserMM(subTemp,kv));
		}
		else if(subTemp.m_tag.equals("CC_UNICOM"))
		{
			m_TempString.append(parser.parsercdr21(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("SM_UNICOM"))
		{
			m_TempString.append(parser.parsercdr22(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("MM_UNICOM"))
		{
			m_TempString.append(parser.parsercdr23(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("SCCP_UNICOM"))
		{
			m_TempString.append(parser.parsercdr24(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("RANAPCC_UNICOM"))
		{
			m_TempString.append(parser.parsercdr51(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("RANAPSM_UNICOM"))
		{
			m_TempString.append(parser.parsercdr52(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("RANAPMM_UNICOM"))
		{
			m_TempString.append(parser.parsercdr53(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("CC_CMCC"))
		{
			m_TempString.append(parser.parsercdr21(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("SM_CMCC"))
		{
			m_TempString.append(parser.parsercdr22(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("MM_CMCC"))
		{
			m_TempString.append(parser.parsercdr23(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("SCCP_CMCC"))
		{
			m_TempString.append(parser.parsercdr24(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("RANAPCC_CMCC"))
		{
			m_TempString.append(parser.parsercdr51(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("RANAPSM_CMCC"))
		{
			m_TempString.append(parser.parsercdr52(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("RANAPMM_CMCC"))
		{
			m_TempString.append(parser.parsercdr53(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("HO_CMCC"))
		{
			m_TempString.append(parser.parserHO(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("HO_UNICOM"))
		{
			m_TempString.append(parser.parserHO(subTemp,kv,_CityID,2));
		}
		else if(subTemp.m_tag.equals("RELOC_CMCC"))
		{
			m_TempString.append(parser.parserRELOC(subTemp,kv,_CityID,1));
		}
		else if(subTemp.m_tag.equals("RELOC_UNICOM"))
		{
			m_TempString.append(parser.parserRELOC(subTemp,kv,_CityID,2));
		}

		
		
		return m_TempString.toString();
	}
	
	private int GetDisTempTableID(String dataType,Map<String,String> kv)
	{
		int id = -1;
		String sTableName = "";
		String sColumnName = "";
		String sValue = "";
		if(dataType.equals("CC"))
		{
			
		}
		else if(dataType.equals("SM"))
		{
			
		}
		else if(dataType.equals("MM"))
		{
		}
		else if(dataType.equals("CC_UNICOM"))
		{
			//ѡ��ַ�ģ��
			sTableName = "cdr21";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				if(sValue.equals("65535"))
				{
					_CityID = 531;
					return -10000;
				}
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("CC_CMCC"))
		{
			//ѡ��ַ�ģ��
			sTableName = "cdr11";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				if(sValue.equals("65535"))
				{
					_CityID = 531;
					return -10000;
				}
				//log.debug("MGW_IP:" + sValue);
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("SM_UNICOM"))
		{
			sTableName = "cdr22";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("SM_CMCC"))
		{
			sTableName = "cdr12";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty() )
			{
				//log.debug("MGW_IP:" + sValue);
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("MM_UNICOM"))
		{
			sTableName = "cdr23";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("MM_CMCC"))
		{
			sTableName = "cdr13";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				//log.debug("MGW_IP:" + sValue);
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("SCCP_UNICOM"))
		{
			sTableName = "cdr24";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("SCCP_CMCC"))
		{
			sTableName = "cdr14";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				//log.debug("MGW_IP:" + sValue);
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
				
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RANAPCC_UNICOM"))
		{
			sTableName = "cdr21";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				if(sValue.equals("65535"))
				{
					_CityID = 531;
					return -10000;
				}
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RANAPCC_CMCC"))
		{
			sTableName = "cdr11";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if( sValue != null && !sValue.isEmpty())
			{
				if(sValue.equals("65535"))
				{
					_CityID = 531;
					return -10000;
				}
				//log.debug("MGW_IP:" + sValue);
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RANAPSM_UNICOM"))
		{
			sTableName = "cdr22";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RANAPSM_CMCC"))
		{
			sTableName = "cdr12";
			sColumnName = "START_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RANAPMM_UNICOM"))
		{
			sTableName = "cdr23";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RANAPMM_CMCC"))
		{
			sTableName = "cdr13";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("HO_UNICOM"))
		{
			sTableName = "cdr23";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("HO_CMCC"))
		{
			sTableName = "cdr13";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RELOC_UNICOM"))
		{
			sTableName = "cdr23";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("RELOC_CMCC"))
		{
			sTableName = "cdr13";
			sColumnName = "DEST_LAC";
			sValue = kv.get(sColumnName);
			if(sValue!=null && !sValue.isEmpty())
			{
				_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
			}
			else
			{
				_CityID = 531;
			}
		}
		
		String sCompare = sTableName + "_" + _CityID;
		DistributeTemplet disTemp = this.distribute.getDisTemplet();
		for(int i = 0;i<disTemp.tableTemplets.values().size();i++)
		{
			TableTemplet disTable = disTemp.tableTemplets.get(i);
			if(disTable.tableName.contains(sCompare))
			{
				id = disTable.tableIndex;
				return id;
			}
			
		}
		
		sCompare = sTableName + "_cmcc";
		for(int i = 0;i<disTemp.tableTemplets.values().size();i++)
		{
			TableTemplet disTable = disTemp.tableTemplets.get(i);
			if(disTable.tableName.contains(sCompare))
			{
				id = disTable.tableIndex;
				return id;
			}
			
		}
		
		sCompare = sTableName + "_unicom";
		for(int i = 0;i<disTemp.tableTemplets.values().size();i++)
		{
			TableTemplet disTable = disTemp.tableTemplets.get(i);
			if(disTable.tableName.contains(sCompare))
			{
				id = disTable.tableIndex;
				return id;
			}
			
		}
		
		return id;
	}
	
	
	
	public static String[] split(String linestr, String splitsign, String upsplitsign)
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
	
	private void UploadIndexFile()
	{
		Long lTime = this.collectObjInfo.getLastCollectTime().getTime() - 24*3600*1000;
		SimpleDateFormat ft1 = new SimpleDateFormat("yyyyMMdd");
		String sTime = ft1.format(new Date(lTime));
		
		for(TableTemplet table:this.distribute.getDisTemplet().tableTemplets.values())
		{
			String filename = String.format("%s_%s.idx", table.tableName,sTime);
			CreateIndexFile(filename);
		}
	}
	
	private boolean CreateIndexFile(String filename)
	{
		boolean blResult = false;
		String logStr;
				
		String ftpIP = this.collectObjInfo.getInDBServerConfig().getInDBServer();
		String ftpuser = this.collectObjInfo.getInDBServerConfig().getInDBUser();
		String ftppwd = this.collectObjInfo.getInDBServerConfig().getInDBPassword();
		
		
		
		FTPTool ftp = new FTPTool(ftpIP,21,ftpuser,ftppwd);
		
		//�ѵ�ǰ�ɼ�����������Ϊ�ɼ�Ŀ¼
		String docName = "";
		ftp.setKeyID("999999999999");
		
		
		logStr = "IDX-UPLOAD����ʼFTP��½.";
		this.log.debug(logStr);
		try
		{
			boolean bOK = ftp.login(30000, 5);
			if (!bOK)
		 	{
				logStr = "IDX-UPLOAD�� FTP��γ��Ե�½ʧ��:" + ftp;
				this.log.error(logStr);
				return blResult;
		 	}
		    logStr = "IDX-UPLOAD�� FTP��½�ɹ�.";
		    this.log.debug("�ַ�����: FTP��½�ɹ�.");
		    
		   
		    
		    File fileindex = new File(SystemConfig.getInstance().getCurrentPath()
		    		+ File.separatorChar + filename);
		    fileindex.createNewFile();
		  
		    int code = ftp.uploadFile(fileindex.getAbsolutePath(), docName);
		    switch(code)
		    {
		    	case 100://�ɹ�
		    		//File sucfile = new File(fileName1X);
		    		if(fileindex.delete())
		    		{
		    			log.debug("�ļ�:[" + filename + " ]ɾ���ɹ�");
		    		}
		    		break;
		    	case 400:
		    		//�쳣
		    		break;
		    	case 401:
		    		//��������ʧ��
		    		break;
		    }
		    	
		}
			catch (Exception e)
		    {
		    	logStr = "�ַ�����: FTP�ɼ��쳣.";
		    	this.log.error(logStr, e);
		    }
		    finally
		    {
		    	ftp.disconnect();
		    }
		return blResult;
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}
