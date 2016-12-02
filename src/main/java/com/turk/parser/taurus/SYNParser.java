package com.turk.parser.taurus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.parser.Parser;
import com.turk.parser.taurus.model.CFG_NUM_HOME;
import com.turk.parser.taurus.model.MOD_CELL;

import com.turk.distributor.DistributeTemplet;
import com.turk.distributor.DistributeTemplet.TableTemplet;
import com.turk.templet.LineTempletP;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class SYNParser extends Parser{
	
	private Logger logger = LogMgr.getInstance().getSystemLogger();
	String remainingData = "";
	
	int nOper = 1;
	
	
	/**
	 * �ַ�ģ����
	 */
	int _DisTableID = -1;
	
	int _CityID = 0;
	
	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		this.logger.debug(this.collectObjInfo.getTaskID() + 
				":Start parser AVL file��" + this.fileName);

		
		FileInputStream fis = null;
		try
		{
		  	String logStr = this + ": starting parse file : " + this.fileName;
		  	this.log.debug(logStr);
		  	this.collectObjInfo.log("����", logStr);

		  	File fs = new File(this.fileName);
		      
		  	if (!fs.exists())
		    {
		  		this.logger.error(this.collectObjInfo.getTaskID() + ":File does not exist��" + this.fileName);
		  		return false;
		    }
		  	
		  	
		  			
		  	fis = new FileInputStream(fs);
		      
		  	@SuppressWarnings("resource")
			BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
		      
		  	String line = reader.readLine();
		  	while (line != null)
		  	{
		  		BuildData(line);
		  		line = reader.readLine();
		  	}

		  	//String strEnd = "\n**FILEEND**";
		  	//BuildData(strEnd.toCharArray(), strEnd.length());
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
	
	public boolean BuildData(String line)
  	{
  		boolean bReturn = true;

  		
  		String logStr = null;

  		try
  		{
  			ParseLineData(line);
  		}
  		catch (Exception e)
  		{
  			bReturn = false;
  			logStr = this + ": Cause:";
  			this.log.error(logStr, e);
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

		

		LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
			.get(nSubTmpIndex);

		if(subTemp.m_tag.equals("SYN_CMCC"))
	  	{
	  		nOper = 1;
	  	}
	  	else if(subTemp.m_tag.equals("SYN_UNICOM"))
	  	{
	  		nOper = 2;
	  	}
		
		StringBuffer strNewRow = new StringBuffer();
		String strValue = "";

		try
		{
			switch (subTemp.m_nParseType)
			{
				case 1:
					strValue = ParseRowBySplit(subTemp, strOldRow);
					if(templet.nScanType == 3)
					{
						//����ĳ���ֶ��������ַ����ļ����ĸ�(�ֵ������)
						nSubTmpIndex = _DisTableID;
					}
					break;
				case 2:
				case 3:
				case 4:
				case 5://AVL����
					this.log.warn("ParseType ���ô��󣬴˴�ӦΪ1 ");
					break;
			}

		}
		catch (Exception e)
		{
			String str = this.collectObjInfo.getTaskID() + " : error when parsing data. templet name : " + 
				templet.tmpName + " data:" + strOldRow;
			this.log.error(str, e);
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
			log.error("ת��UTF-8��ʽ�쳣",e);
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
					log.error("���������ݼ����ֵ��ʱ����",ex);
					//kv.put(subTemp.m_Filed.get(k).m_strFieldName, removeNoiseSemicolon(m_strTemp[k].trim()));
				}
			}
			nCount++;
		}
		
		if(templet.nScanType == 3)
		{
			_DisTableID = GetDisTempTableID(subTemp.m_tag,kv);
		}
		
		
		
		
		String strTime = "";
		if(kv.get("CALLDATE")!=null)
		{
			SimpleDateFormat sdFormatIn1 = new SimpleDateFormat("yyMMdd");
			SimpleDateFormat sdFormatIn2 = new SimpleDateFormat("HHmmss");
			
			SimpleDateFormat sdFormatOut1 = new SimpleDateFormat("yyyy-MM-dd");
			SimpleDateFormat sdFormatOut2 = new SimpleDateFormat("HH:mm:ss");
			
			String calldate = kv.get("CALLDATE");
			String calltime = kv.get("CALLTIME");
			try {
				Date date = sdFormatIn1.parse(calldate);
				Date time = sdFormatIn2.parse(calltime);
				strTime = sdFormatOut1.format(date) + " " + sdFormatOut2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				this.logger.error("����START_TIME��ʱ���ʽ�쳣",e);
			}
		}
		else if(kv.get("START_TIME")!=null)
		{
			SimpleDateFormat sdFormatIn1 = new SimpleDateFormat("yyyyMMddHHmmss");
			SimpleDateFormat sdFormatOut1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String starttime = kv.get("START_TIME");
			try {
				Date date = sdFormatIn1.parse(starttime);
				strTime = sdFormatOut1.format(date);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				this.logger.error("����START_TIME��ʱ���ʽ�쳣",e);
			}
		}
		
		//��ʼʱ��	start_time	start_time	
		m_TempString.append(strTime + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��	end_time
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��	cdr_id		
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSC	bsc		
		m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MSC	msc		
		m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP	mgw_ip
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP	msc_ip		
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������	service_type
		m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø�������	mm_type	�̶�Ϊ���ȵ�Ǽ�
		m_TempString.append("110" + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���	mm_result
		m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��¾ܾ�ԭ��	lu_reject_cause
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		
		Long imsi = -10000L;
		String msisdn = "-10000";
		String strimsi = Util.findByRegex(kv.get("MSIMSI_1"), "[0-9]*", 0);
		if(strimsi != null)
		{
			if(!kv.get("MSIMSI_1").isEmpty())
				imsi = Long.valueOf(kv.get("MSIMSI_1"));
		}
			
		if(msisdn.isEmpty()|| msisdn == "-10000")
		{
			msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
		}
		
		//MSISDN	msisdn	msisdn
		m_TempString.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI	imsi	imsi	
		m_TempString.append(imsi + subTemp.m_strNewFieldSplitSign);
		
		//IMEI	imei	imei
		String strimei = kv.get("MSIMEI_1");
		//if(strimei!=null && strimei.length() > 14)
		//	msisdn = msisdn.substring(0,13);
		
		m_TempString.append(strimei + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI	first_tmsi	
		m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI	last_tmsi
		m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MCC	mcc
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MNC	mnc
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC	sour_lac	lac_1	����CTJSTID��mod_cell���ҵ�bts_name��Ӧ��lac
		//λ�ø���ǰCI	sour_ci	ci_1	����CTJSTID��mod_cell���ҵ�bts_name��Ӧ��ci
		//λ�ø��º�LAC	dest_lac	lac_1	����CTJSTID��mod_cell���ҵ�bts_name��Ӧ��lac
		//λ�ø��º�CI	dest_ci	ci_1	����CTJSTID��mod_cell���ҵ�bts_name��Ӧ��ci
		String ctjstid = kv.get("CTJSTID");
		MOD_CELL cellinfo = MapModCell.getInstance().getCellInfoByCTJSTIDANDOper(nOper,ctjstid);
		if(cellinfo!=null)
		{
			m_TempString.append(cellinfo.getLAC() + subTemp.m_strNewFieldSplitSign);
			m_TempString.append(cellinfo.getCI() + subTemp.m_strNewFieldSplitSign);
			m_TempString.append(cellinfo.getLAC() + subTemp.m_strNewFieldSplitSign);
			m_TempString.append(cellinfo.getCI() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//�������ԭ��	clearrequest_cause
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		//�������ԭ��	clearcommand_cause		
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		//λ�ø������ʱ��	resp_delay		
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		//BSSMAP���ʱ��	clear_delay		
		//m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		
		
		
		if(cellinfo!=null)
		{
			//����ID	city_id	city_id	
			m_TempString.append(cellinfo.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//cell_sys_id	С��ϵͳ��	bigint	����CTJSTID��mod_cell���ҵ�bts_name��Ӧ��cell_sys_id
			m_TempString.append(cellinfo.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			
			//������������ID
			
			int sectionno = 0;
			String str = Util.findByRegex(msisdn, "[0-9]*", 0);
			if(str!=null)
			{
				if(msisdn.length() >= 7)
				{
					sectionno = Integer.parseInt(String.valueOf(msisdn).substring(0,7));
				}
			}
			CFG_NUM_HOME home1 = NumHome.getInstance().getRegion(sectionno);
			if(home1 != null)
			{
				//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
				m_TempString.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
			}
			
			//cell_name	С������	character varying(32)	����CTJSTID��mod_cell���ҵ�bts_name��Ӧ��cell_name
			m_TempString.append(cellinfo.getCellName() + subTemp.m_strNewFieldSplitSign);
			
			
		}
		else
		{
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		}

		return m_TempString.toString();
	}
	
	
	private int GetDisTempTableID(String dataType,Map<String,String> kv)
	{
		int id = -1;
		String sTableName = "";
		String sColumnName = "";
		String sValue = "";
		
		if(dataType.equals("SYN_UNICOM"))
		{
			//ѡ��ַ�ģ��
			sTableName = "cdr23";
			sColumnName = "CTJSTID";
			sValue = kv.get(sColumnName);
			
			MOD_CELL cellinfo = MapModCell.getInstance().getCellInfoByCTJSTIDANDOper(2,sValue);
			if(cellinfo!=null)
			{
				_CityID = cellinfo.getCityID();
			}
			else
			{
				_CityID = 531;
			}
		}
		else if(dataType.equals("SYN_CMCC"))
		{
			//ѡ��ַ�ģ��
			sTableName = "cdr13";
			sColumnName = "CTJSTID";
			sValue = kv.get(sColumnName);
			MOD_CELL cellinfo = MapModCell.getInstance().getCellInfoByCTJSTIDANDOper(1,sValue);
			if(cellinfo!=null)
			{
				_CityID = cellinfo.getCityID();
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

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}
