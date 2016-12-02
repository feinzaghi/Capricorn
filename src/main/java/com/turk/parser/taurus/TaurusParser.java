package com.turk.parser.taurus;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.parser.Parser;
import com.turk.parser.taurus.model.CFG_NUM_HOME;
import com.turk.parser.taurus.model.MOD_CELL;
import com.turk.specialapp.taurus.utele.MessageQueue;
import com.turk.specialapp.taurus.utele.SendResult;
import com.turk.templet.LineTempletP;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class TaurusParser {
	
	private MonitorHit _monitor;
	private Logger applog = LogMgr.getInstance().getAppLogger("taurus");
	
	public TaurusParser(MonitorHit monitor, Parser objParser)
	{
		_monitor = monitor;
		objParser.getCollectObjInfo();
	}
	
	
	public String parserCC(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//��������	mo_mt	"��call_type����
		strLine.append(kv.get("CALL_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//��������msisdn	character varying(32)	ͨ����IMSI����map_imsi_msisdn���ѯ����Ӧ�ĵ绰����
		Long imsi = -10000L;
		String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
		if(strimsi != null)
		{
			if(!kv.get("IMSI").isEmpty())
				imsi = Long.valueOf(kv.get("IMSI"));
		}
		
		
		String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//�Է�����	mt_msisdn	character varying(32)	"����������Ϊ0�������к��롿 ����������Ϊ1�������к��롿
		int mo_mt = 0;
		String sMo_mt = Util.findByRegex(kv.get("CALL_TYPE"), "[0-9]*", 0);
		if(sMo_mt != null)
			mo_mt = Integer.parseInt(sMo_mt);
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0�������к��롿
			//������ʽ���ж��Ƿ�Ϊ����
			//String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{//1�������к��롿
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}

		//����IMSI	imsi	bigint	��IMSI��
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//�����豸��	esn	character varying(32)	��ESN��
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//ʱ��	call_duration	integer	�����ʱ�ӡ���ȡ��
		strLine.append(kv.get("CLEAR_TIME") + subTemp.m_strNewFieldSplitSign);
		
		int sectionno = 0;
		String str = Util.findByRegex(msisdn, "[0-9]*", 0);
		if(str!=null)
		{
			if(msisdn.length() >= 7)
			{
				sectionno = Integer.parseInt(String.valueOf(msisdn).substring(0,7));
			}
		}
		
		String belongCity = "0";
		CFG_NUM_HOME home1 = NumHome.getInstance().getRegion(sectionno);
		if(home1 != null)
		{
			//��������ʡ��	province	character varying(32)	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home���������������ʡ��
			strLine.append(home1.getProvince() + subTemp.m_strNewFieldSplitSign);
			
			//������������	city	character varying(32)	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���
			strLine.append(home1.getCity() + subTemp.m_strNewFieldSplitSign);
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//����������Ӫ��	operator	character varying(32)	��ʱΪ��
			strLine.append("3" + subTemp.m_strNewFieldSplitSign);
			

			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		sectionno = 0;
		str = Util.findByRegex(mt_msisdn, "[0-9]*", 0);
		if(str != null)
		{
			if(mt_msisdn.length()>=7)
			{
				sectionno = Integer.parseInt(String.valueOf(mt_msisdn).substring(0,7));
			}
		}
		CFG_NUM_HOME home2 = NumHome.getInstance().getRegion(sectionno);
		
		if(home2 != null)
		{
			//�Է���������	mt_country	character varying(32)	��ʱΪ��
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			
			//�Է�����ʡ��	mt_province	character varying(32)	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home���������������ʡ��
			strLine.append(home2.getProvince() + subTemp.m_strNewFieldSplitSign);
			//�Է���������	mt_city	character varying(32)	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���
			strLine.append(home2.getCity() + subTemp.m_strNewFieldSplitSign);
			
			//�Է���������ID	o_city_id	smallint	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//�Է�������Ӫ��	mt_operator	character varying(32)	��ʱΪ��
			strLine.append(home2.getCardType() + subTemp.m_strNewFieldSplitSign);
			
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		String sOPC = kv.get("OPC");
		int nCityID = 531;
		
		//����CI	ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�
		MOD_CELL cell = null;
		if(sOPC.equals("-1"))
		{//����
			int sIP = Integer.parseInt(kv.get("S_IP"));
			int value = sIP/65536;
			nCityID = MapNE2City.getInstance().getCityID(value);
			
			long ci = Long.parseLong(kv.get("S_IP"));
			cell = MapModCell.getInstance().getCellInfo(ci);
			if(cell != null && cell.getCityID() == nCityID)
			{
				strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				String ip = Util.int2ip(ci);
				String[] numArray = ip.split("\\.");
				String cellname = "";
				long lNeSysID = -10000;
				if(numArray.length==4)
				{
					int LAC= Integer.parseInt(numArray[1]) * 256 + Integer.parseInt(numArray[0]);
					int CI = Integer.parseInt(numArray[3]) * 256 + Integer.parseInt(numArray[2]);
					cellname = LAC+"_"+CI;
					
					lNeSysID = 3*10000000000000L 
							+ (long)nCityID*10000000000L  
							+ (long)LAC * 100000L
							+ (long)CI;
				}
				strLine.append(cellname + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
				strLine.append(lNeSysID + subTemp.m_strNewFieldSplitSign);
			}
		}
		else
		{//����
			String sCI = kv.get("START_CI");
			nCityID = MapNE2City.getInstance().getCityID(Integer.parseInt(sOPC));
			cell = MapModCell.getInstance().getTelecomCellInfo(sCI);
			if(cell!=null)
			{				
				strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCityID() + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
				long lNeSysID = 3*10000000000000L + 
						(long)nCityID*10000000000L + Long.parseLong(sCI);
				strLine.append(lNeSysID + subTemp.m_strNewFieldSplitSign);
			}
		}
		
		//ԴIP	s_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��IP	d_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI	tmsi_o	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI	tmsi_d	integer	tmsi_d
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//ԴLAC	sour_lac	integer	sour_lac
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ԴCI	sour_ci	integer	sour_ci
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC	start_lac	integer	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI	start_ci	integer	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ֹ��LAC	end_lac	integer	end_lac
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ֹ��CI	end_ci	integer	end_ci
		strLine.append(kv.get("END_CI") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��LAC	dest_lac	integer	dest_lac
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��CI	dest_ci	integer	dest_ci
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���	a_result	smallint	a_result
		strLine.append(kv.get("A_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//BSC�ڲ��л�����	ho_num	smallint	ho_num
		strLine.append(kv.get("HO_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//���ԭ��	clear_cause	smallint	clear_cause
		strLine.append(kv.get("CLEAR_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�û��ػ�	a_close_reg	smallint	a_close_reg
		strLine.append(kv.get("A_CLOSE_REG") + subTemp.m_strNewFieldSplitSign);
		
		//Ӧ��ʱ��	conn_time	smallint	conn_time
		strLine.append(kv.get("CONN_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//������Ϣ����	fl_num	smallint	fl_num
		strLine.append(kv.get("FL_NUM") + subTemp.m_strNewFieldSplitSign);

		
		try {
			//���
			if(_monitor!=null)
			{
				String starttime = kv.get("START_TIME");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
				String cellname = cell==null?"":cell.getCellName();
				long cellsysid = cell==null?0:cell.getCellSysID();
				_monitor.IsTouchNet(msisdn, mt_msisdn, formatter.parse(starttime), 
							cellname,cellsysid, mo_mt, callduration, 31);
				_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, mt_msisdn);
				
				_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, 0, cellsysid);
			}
			
			String lon = "null";
			String lat = "null";
			if(cell != null)
			{
				lon = String.valueOf(cell.getLon());
				lat = String.valueOf(cell.getLat());
			}
			
			
			//ʵʱ�û�����
			String data = "CC_TELECOM:" + strLine 
					+ lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			SocketMonitor(strimsi,msisdn,data);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("��ؼ�¼�쳣",e);
		}
		
		if(subTemp.m_hasRowkey)
		{
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String rowkey = String.format("%s_%s_%s_%s", 
					String.valueOf(imsi),msisdn,mt_msisdn,starttime);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}
		
		return strLine.toString();
	}
	
	public String parserSM(LineTempletP.SubTemplet subTemp,Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//mo_mt	smallint	"��sms_type����0�����ŷ���MO��1�����Ž���MT��"
		strLine.append(kv.get("SMS_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		int mo_mt = 0;
		
		if(kv.get("SMS_TYPE") != null)
			mo_mt = Integer.parseInt(kv.get("SMS_TYPE"));
		
		//msisdn	character varying(32)	ͨ����IMSI����map_imsi_msisdn���ѯ����Ӧ�ĵ绰����
		Long imsi = -10000L;
		String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
		if(strimsi != null)
		{
			if(!kv.get("IMSI").isEmpty())
				imsi = Long.valueOf(kv.get("IMSI"));
		}
		
		String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//o_msisdn	character varying(32)	"����������Ϊ0�������к��롿
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0�������к��롿
			//������ʽ���ж��Ƿ�Ϊ����
			//String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
			
		}
		else if(mo_mt == 1)
		{//1�������к��롿
			//String str = Util.findByRegex(kv.get("CALLING"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}
		
		//imsi	bigint	��imsi��
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//esn	character varying(32)	��esn��
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//result	smallint	��mm_result����0��ʾ�ɹ���1��ʾʧ��
		strLine.append(kv.get("SMS_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//province	character varying(32)	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home���������������ʡ��
		//city	character varying(32)	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���
		//operator	character varying(32)	��ʱΪ��
		int sectionno = 0;
		if(msisdn.length() >= 7)
		{
			sectionno = Integer.parseInt(String.valueOf(msisdn).substring(0,7));
		}
		
		String belongCity = "0";
		
		CFG_NUM_HOME home1 = NumHome.getInstance().getRegion(sectionno);
		if(home1 != null)
		{
			//��������ʡ��	province	character varying(32)	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home���������������ʡ��
			strLine.append(home1.getProvince() + subTemp.m_strNewFieldSplitSign);
			
			//������������	city	character varying(32)	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���
			strLine.append(home1.getCity() + subTemp.m_strNewFieldSplitSign);
			
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//����������Ӫ��	operator	character varying(32)	��ʱΪ��
			strLine.append("3" + subTemp.m_strNewFieldSplitSign);
			
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//o_country	character varying(32)	��ʱΪ��
		//o_province	character varying(32)	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home���������������ʡ��
		//o_city	character varying(32)	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���
		//o_operator	character varying(32)	��ʱΪ��
		
		sectionno = 0;
		String str = Util.findByRegex(mt_msisdn, "[0-9]*", 0);
		if(str != null)
		{
			if(mt_msisdn.length()>=7)
			{
				sectionno = Integer.parseInt(String.valueOf(mt_msisdn).substring(0,7));
			}
		}
		CFG_NUM_HOME home2 = NumHome.getInstance().getRegion(sectionno);
		
		if(home2 != null)
		{
			//�Է���������	mt_country	character varying(32)	��ʱΪ��
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			
			//�Է�����ʡ��	mt_province	character varying(32)	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home���������������ʡ��
			strLine.append(home2.getProvince() + subTemp.m_strNewFieldSplitSign);
			//�Է���������	mt_city	character varying(32)	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���
			strLine.append(home2.getCity() + subTemp.m_strNewFieldSplitSign);
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//�Է�������Ӫ��	mt_operator	character varying(32)	��ʱΪ��
			strLine.append(home2.getCardType() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//����CI	ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�
		String sOPC = kv.get("OPC");
		int nCityID = 531;
		
		//����CI	ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�
		MOD_CELL cell = null;
		int sIP = Integer.parseInt(kv.get("S_IP"));
		if(sOPC.equals("-1"))
		{//����
			int value = sIP/65536;
			nCityID = MapNE2City.getInstance().getCityID(value);
			
			long ci = Long.parseLong(kv.get("S_IP"));
			cell = MapModCell.getInstance().getCellInfo(ci);
			if(cell != null && cell.getCityID() == nCityID)
			{
				strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				String ip = Util.int2ip(ci);
				String[] numArray = ip.split("\\.");
				String cellname = "";
				long lNeSysID = -10000;
				if(numArray.length==4)
				{
					int LAC= Integer.parseInt(numArray[1]) * 256 + Integer.parseInt(numArray[0]);
					int CI = Integer.parseInt(numArray[3]) * 256 + Integer.parseInt(numArray[2]);
					cellname = LAC+"_"+CI;
					
					lNeSysID = 3*10000000000000L 
							+ (long)nCityID*10000000000L  
							+ (long)LAC * 100000L
							+ (long)CI;
				}
				strLine.append(cellname + subTemp.m_strNewFieldSplitSign);
				strLine.append(lNeSysID + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
				
			}
		}
		else
		{//����
			String sCI = kv.get("START_CI");
			nCityID = MapNE2City.getInstance().getCityID(Integer.parseInt(sOPC));
			cell = MapModCell.getInstance().getTelecomCellInfo(sCI);
			if(cell!=null)
			{				
				strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCityID() + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
				long lNeSysID = 3*10000000000000L + 
						(long)nCityID*10000000000L + Long.parseLong(sCI);
				strLine.append(lNeSysID + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
			}
		}
		
		//ԴIP	s_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��IP	d_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC	start_lac	integer	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI	start_ci	integer	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI	tmsi_o	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
	
		try {
				if(_monitor!=null)
				{
					//���
					String starttime = kv.get("START_TIME");
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
					String cellname = cell==null?"":cell.getCellName();
					long cellsysid = cell==null?0:cell.getCellSysID();
				
					_monitor.IsTouchNet(msisdn, mt_msisdn, formatter.parse(starttime), 
							cellname, cellsysid, mo_mt, 0, 32);
					
					_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, mt_msisdn);
					
					_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, 0, cellsysid);
				}
				
				String lon = "null";
				String lat = "null";
				if(cell != null)
				{
					lon = String.valueOf(cell.getLon());
					lat = String.valueOf(cell.getLat());
				}
				
				//ʵʱ�û�����
				
				String data = "SM_TELECOM:" + strLine
						+ lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(strimsi,msisdn,data);
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("��ؼ�¼�쳣",e);
			}
			
		if(subTemp.m_hasRowkey)
		{
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String rowkey = String.format("%s_%s_%s_%s", 
					String.valueOf(imsi),msisdn,mt_msisdn,starttime);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}
		
		return strLine.toString();
	}
	
	public String parserMM(LineTempletP.SubTemplet subTemp,Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//reg_type	smallint	"��mm_type����
		//0 ����ʱ��
		//1 ����
		//2 ��������
		//3 �ػ�
		//4 ��������
		///5 ����ָ��
		//6 ���ھ���
		//7 �����û���
		//9 BCMC�Ǽ�"	��mm_type��
		strLine.append(kv.get("MM_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//msisdn	character varying(32)	ͨ��IMSI����map_imsi_msisdn���ѯ����Ӧ�ĵ绰����	��mdn����map_imsi_msisdn���ѯ����Ӧ�ĵ绰����
		Long imsi = -10000L;
		if(!kv.get("IMSI").isEmpty())
		{
			String str = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
			if(str != null)
				imsi = Long.valueOf(kv.get("IMSI"));
		}
		String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
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
		String belongCity = "0";
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//imsi	bigint	"��mdn��4600��ͷ��"	��mdn��
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//esn	character varying(32)	��esn��	��esn��
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		//ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�

		//����CI	ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�
				
		String sOPC = kv.get("OPC");
		int nCityID = 531;
		
		//����CI	ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�
		MOD_CELL cell = null;
		int sIP = Integer.parseInt(kv.get("S_IP"));
		if(sOPC.equals("-1"))
		{//����
			int value = sIP/65536;
			nCityID = MapNE2City.getInstance().getCityID(value);
			
			long ci = Long.parseLong(kv.get("S_IP"));
			cell = MapModCell.getInstance().getCellInfo(ci);
			if(cell != null && cell.getCityID() == nCityID)
			{
				strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
				strLine.append("3" + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				String ip = Util.int2ip(ci);
				String[] numArray = ip.split("\\.");
				String cellname = "";
				long lNeSysID = -10000;
				if(numArray.length==4)
				{
					int LAC= Integer.parseInt(numArray[1]) * 256 + Integer.parseInt(numArray[0]);
					int CI = Integer.parseInt(numArray[3]) * 256 + Integer.parseInt(numArray[2]);
					cellname = LAC+"_"+CI;
					
					lNeSysID = 3*10000000000000L 
							+ (long)nCityID*10000000000L  
							+ (long)LAC * 100000L
							+ (long)CI;
				}
				strLine.append(cellname + subTemp.m_strNewFieldSplitSign);
				strLine.append("3" + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
				strLine.append(lNeSysID + subTemp.m_strNewFieldSplitSign);
			}
		}
		else
		{//����
			String sCI = kv.get("START_CI");
			nCityID = MapNE2City.getInstance().getCityID(Integer.parseInt(sOPC));
			cell = MapModCell.getInstance().getTelecomCellInfo(sCI);
			if(cell!=null)
			{				
				strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
				strLine.append("3" + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCityID() + subTemp.m_strNewFieldSplitSign);
				strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
				strLine.append("3" + subTemp.m_strNewFieldSplitSign);
				strLine.append(nCityID + subTemp.m_strNewFieldSplitSign);
				long lNeSysID = 3*10000000000000L + 
						(long)nCityID*10000000000L + Long.parseLong(sCI);
				strLine.append(lNeSysID + subTemp.m_strNewFieldSplitSign);
			}
		}
		
		//ԴIP	s_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��IP	d_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI	tmsi_o	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI	tmsi_d	integer	tmsi_d
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC	start_lac	integer	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI	start_ci	integer	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//mm_result	smallint	mm_result
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		
		try {
				if(_monitor!=null)
				{	//���
					String starttime = kv.get("START_TIME");
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
					String cellname = cell==null?"":cell.getCellName();
					int mmtype = Integer.parseInt(kv.get("MM_TYPE"));
					long cellsysid = cell==null?0:cell.getCellSysID();
					
					_monitor.IsTouchNet(msisdn, "", formatter.parse(starttime), 
								cellname, cellsysid, mmtype, 0, 33);
					
					_monitor.IsPowerUp(msisdn, formatter.parse(starttime), cellname, cellsysid, mmtype);
					
					_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, 0, cellsysid);
				}	
				
				String lon = "null";
				String lat = "null";
				if(cell != null)
				{
					lon = String.valueOf(cell.getLon());
					lat = String.valueOf(cell.getLat());
				}
				
				//ʵʱ�û�����
				
				String data = "MM_TELECOM:" + strLine
						+ lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("��ؼ�¼�쳣",e);
			}
			
		if(subTemp.m_hasRowkey)
		{
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String rowkey = String.format("%s_%s_%s", 
					String.valueOf(imsi),msisdn,starttime);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}
		
		return strLine.toString();
	}
	
	/**
	 * UNICOM CC
	 * @param subTemp
	 * @param kv
	 * @return
	 */
	public String parsercdr21(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		strLine.append(kv.get("CALL_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//��֪��������
		//strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
		int mo_mt = 0;
		String sMo_mt = Util.findByRegex(kv.get("CALL_TYPE"), "[0-9]*", 0);
		if(sMo_mt!=null)
			mo_mt = Integer.parseInt(sMo_mt);
		String msisdn = "-10000";
		if(mo_mt == 0 || mo_mt == 3 || mo_mt == 4)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//�Է�IMSI
		//�Է�IMEI
		//�Է�����
		String o_msisdn = "-10000";
		Long o_imsi = -10000L;
		if(mo_mt == 0 || mo_mt == 3 || mo_mt == 4)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");

			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//���Ӻ���
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//ǰת����
		//strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		//strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ֹ��LAC
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ֹ��CI
		strLine.append(kv.get("END_CI") + subTemp.m_strNewFieldSplitSign);
		
		//ԴLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ԴCI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//��·ʱ϶��TS
		//strLine.append(kv.get("CIC5") + subTemp.m_strNewFieldSplitSign);
		
		//��·ʶ����PCM
		//strLine.append(kv.get("CIC7") + subTemp.m_strNewFieldSplitSign);
		
		//�л�����
		strLine.append(kv.get("HO_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//�л�ԭ��
		//strLine.append(kv.get("HO_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�л����
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���1
		//strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���2
		//strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ܾ�ԭ��
		//strLine.append(kv.get("HOREJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�л�ʧ��ԭ��
		//strLine.append(kv.get("HOFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//����ԭ��
		//strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//����������
		//strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����Ѱ����Ӧʱ��
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//���ӣ�Ӧ��ʱ��
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		//strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//�����Ÿ���
		//strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//���б��ִ���
		//strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//���лָ�����
		//strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF��������
		//strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF�ܾ�����
		//strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//ͨ��ʱ��
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//TCHռ��ʱ��
		//strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = oper + "_" + kv.get("START_LAC") + "_" + kv.get("START_CI");
		String sLacCI = kv.get("START_LAC") + "_" + kv.get("START_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		long lCellSysID = 0L;
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			lCellSysID = cell.getCellSysID();
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("START_CI"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				lCellSysID = oper*10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("START_LAC")) * 100000L
						+ Long.parseLong(kv.get("START_CI"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
		
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
		String belongCity = "0";
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//�Է���������ID
		sectionno = 0;
		str = Util.findByRegex(o_msisdn, "[0-9]*", 0);
		if(str!=null)
		{
			if(o_msisdn.length() >= 7)
			{
				sectionno = Integer.parseInt(String.valueOf(o_msisdn).substring(0,7));
			}
		}
		CFG_NUM_HOME home2 = NumHome.getInstance().getRegion(sectionno);
		if(home2 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
				if(_monitor!=null)
				{
					//���
					String starttime = kv.get("START_TIME");
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					int callduration = kv.get("TALK_TIME")==null?0:Integer.parseInt(kv.get("TALK_TIME"));
					String cellname = cell==null?sLacCI:cell.getCellName();
					long cellsysid = lCellSysID;
					_monitor.IsTouchNet(msisdn, o_msisdn, formatter.parse(starttime), 
								cellname,cellsysid, mo_mt, callduration, oper*10+1);
					_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, o_msisdn);
					
					_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					String sLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
					int nLac = -1;
					if(sLac!=null)
						nLac = Integer.parseInt(kv.get("START_LAC"));
					
					_monitor.IsExitLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					_monitor.IsEnterLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					
					_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				}
				
				String lon = "null";
				String lat = "null";
				if(cell != null)
				{
					lon = String.valueOf(cell.getLon());
					lat = String.valueOf(cell.getLat());
				}
				
				//ʵʱ�û�����
				
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("��ؼ�¼�쳣",e);
			}

		if(subTemp.m_hasRowkey)
		{
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String rowkey = String.format("%s_%s_%s_%s_%s_%d", 
					starttime,String.valueOf(imsi),msisdn,o_imsi,o_msisdn,lCellSysID);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}
		
		return strLine.toString();
	}
	
	/**
	 * UNCIOM SM
	 * @param subTemp
	 * @param kv
	 * @return
	 */
	public String parsercdr22(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		strLine.append(kv.get("SMS_TYPE") + subTemp.m_strNewFieldSplitSign);
		int smstype = Integer.parseInt(kv.get("SMS_TYPE"));
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ĺ���
		//strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
		int servicetype = Integer.parseInt(kv.get("SERVICE_TYPE"));
		String msisdn = "-10000";
		if(servicetype == 6)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(servicetype == 7)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//�Է�IMSI
		//�Է�IMEI
		//�Է�����
		String o_msisdn = "-10000";
		Long o_imsi = -10000L;
		if(servicetype == 6)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(servicetype == 7)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
			
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//����ҵ��ģʽ
		//strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//���ų���
		//strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		//strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����Ѱ����Ӧʱ��
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		//strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//���ŷ���/������ʱ��
		//strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = oper + "_" + kv.get("START_LAC") + "_" + kv.get("START_CI");
		String sLacCI = kv.get("START_LAC") + "_" + kv.get("START_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		long lCellSysID = 0L;
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			lCellSysID = cell.getCellSysID();
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("START_CI"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				lCellSysID = oper*10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("START_LAC")) * 100000L
						+ Long.parseLong(kv.get("START_CI"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
		
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
		String belongCity = "0";
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//�Է���������ID
		sectionno = 0;
		str = Util.findByRegex(o_msisdn, "[0-9]*", 0);
		if(str!=null)
		{
			if(o_msisdn.length() >= 7)
			{
				sectionno = Integer.parseInt(String.valueOf(o_msisdn).substring(0,7));
			}
		}
		CFG_NUM_HOME home2 = NumHome.getInstance().getRegion(sectionno);
		if(home2 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
				if(_monitor!=null)
				{
					//���
					String starttime = kv.get("START_TIME");
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
					String cellname = cell==null?sLacCI:cell.getCellName();
					long cellsysid = lCellSysID;
				
					_monitor.IsTouchNet(msisdn, o_msisdn, formatter.parse(starttime), 
							cellname, cellsysid, smstype, 0, oper*10+2);
					
					_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, o_msisdn);
					
					_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					String sLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
					int nLac = -1;
					if(sLac!=null)
						nLac = Integer.parseInt(kv.get("START_LAC"));
					
					_monitor.IsExitLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					_monitor.IsEnterLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					
					_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				}
				
				String lon = "null";
				String lat = "null";
				if(cell != null)
				{
					lon = String.valueOf(cell.getLon());
					lat = String.valueOf(cell.getLat());
				}
				
				//ʵʱ�û�����
				
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("��ؼ�¼�쳣",e);
			}
		
		if(subTemp.m_hasRowkey)
		{//����Hbase��Ҫ������rowkey�ֶ�
			//SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			String rowkey = String.format("%s_%s_%s_%s_%s_%d", 
					starttime,String.valueOf(imsi),msisdn,o_imsi,o_msisdn,lCellSysID);

			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}

		return strLine.toString();
	}
		
	
	public String parsercdr23(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø�������
		strLine.append(kv.get("MM_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��¾ܾ�ԭ��
		//strLine.append(kv.get("LU_REJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN
		String msisdn = kv.get("MSISDN");
		if(msisdn.length() >= 11)
			msisdn = msisdn.substring(msisdn.length() - 11);

		
		Long imsi = -10000L;
		String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
		if(strimsi != null && !kv.get("IMSI").isEmpty())
		{
			imsi = Long.valueOf(kv.get("IMSI"));
		}
		if((msisdn.isEmpty() || msisdn.equals("-10000")) && imsi!=-10000L)
		{
			msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
		}
		
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI
		strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
		//IMEI
		strLine.append(kv.get("IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰCI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø������ʱ��
		//strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		//strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = oper + "_" +  kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		String sLacCI = kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		long lCellSysID = 0L;
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			lCellSysID = cell.getCellSysID();
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("DEST_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("DEST_CI"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				lCellSysID = oper*10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("DEST_LAC")) * 100000L
						+ Long.parseLong(kv.get("DEST_CI"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
		
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
		String belongCity = "0";
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
			if(_monitor!=null)
			{
				//���
				String starttime = kv.get("START_TIME");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
				String cellname = cell==null?sLacCI:cell.getCellName();
				int mmtype = Integer.parseInt(kv.get("MM_TYPE"));
				long cellsysid = lCellSysID;
				
				_monitor.IsTouchNet(msisdn, "", formatter.parse(starttime), 
							cellname, cellsysid, mmtype, 0, oper*10+3);
				
				_monitor.IsPowerUp(msisdn, formatter.parse(starttime), cellname, cellsysid, mmtype);
				
				_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				String sLac = Util.findByRegex(kv.get("DEST_LAC"), "[0-9]*", 0);
				int nLac = -1;
				if(sLac!=null)
					nLac = Integer.parseInt(kv.get("DEST_LAC"));
				
				_monitor.IsExitLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				_monitor.IsEnterLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				
				_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
			}
			
			String lon = "null";
			String lat = "null";
			if(cell != null)
			{
				lon = String.valueOf(cell.getLon());
				lat = String.valueOf(cell.getLat());
			}
			
			//ʵʱ�û�����
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("��ؼ�¼�쳣",e);
		}

		if(subTemp.m_hasRowkey)
		{//����Hbase��Ҫ������rowkey�ֶ�
			//SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			

			String rowkey = String.format("%s_%s_%s_%d", 
					starttime,String.valueOf(imsi),msisdn,lCellSysID);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}
		
		return strLine.toString();
	}
	
	public String parsercdr24(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC/RNC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ��ҵ������
		String result = kv.get("RESULT");
		if(result.equals("65535"))
			result = "255";
		strLine.append(result + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		strLine.append(kv.get("CALLED_IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//����IMEI
		strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//���к���
		strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�������ڵ�rncID
		strLine.append(kv.get("RNC_ID") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����Ѱ����Ӧʱ��
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//������Ϣ
		strLine.append(kv.get("NET_INFO") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = oper + "_" +  kv.get("START_LAC") + "_" + kv.get("START_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("START_CI"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				long lCellSysID = oper*10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("START_LAC")) * 100000L
						+ Long.parseLong(kv.get("START_CI"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
		
		//������������ID
		String msisdn = kv.get("MSISDN");
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
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}

		
		return strLine.toString();
	}
	
	public String parsercdr51(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		strLine.append(kv.get("CALL_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//��֪��������
		//strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
		int mo_mt = Integer.parseInt(kv.get("CALL_TYPE"));
		String msisdn = "-10000";
		if(mo_mt == 0 || mo_mt == 3 || mo_mt == 4)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//�Է�IMSI
		//�Է�IMEI
		//�Է�����
		String o_msisdn = "-10000";
		Long o_imsi = -10000L;
		if(mo_mt == 0 || mo_mt == 3 || mo_mt == 4)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//���Ӻ���
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//ǰת����
		//strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		//strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI
		strLine.append(kv.get("START_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ֹ��LAC
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ֹ��CI
		strLine.append(kv.get("END_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//ԴLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ԴCI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//Ŀ��CI
		strLine.append(kv.get("DEST_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		
		//��·ʱ϶��TS	cic5	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//��·ʶ����PCM	cic7	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//�л�����
		strLine.append(kv.get("INFO_TRANS_CAP") + subTemp.m_strNewFieldSplitSign);
						
		//�л�ԭ��
		//strLine.append(kv.get("RELOC_CAUSE") + subTemp.m_strNewFieldSplitSign);
				
		//�л����
		strLine.append(kv.get("RELOC_RESULT") + subTemp.m_strNewFieldSplitSign);
				
		//�л��ο���1
		//strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
				
		//�л��ο���2
		//strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
				
		//�л��ܾ�ԭ��	horeject_cause	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//�л�ʧ��ԭ��
		//strLine.append(kv.get("RELOCFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
				
		//����ԭ��
		//strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
				
		//����������
		//strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
				
		//�������ԭ��	clearrequest_cause	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		//�������ԭ��	clearcommand_cause	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//�û�Ѱ����Ӧʱ��
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//����Ѱ����Ӧʱ��
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//����ʱ��
		//strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//����ʱ��
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//���ӣ�Ӧ��ʱ��
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//����ʱ��
		//strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//BSSMAP���ʱ��	clear_delay	integer
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//�����Ÿ���
		//strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//���б��ִ���
		//strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//���лָ�����
		//strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//DTMF��������
		//strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//DTMF�ܾ�����
		//strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
						
				
		//ͨ��ʱ��
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
				
		//ռ��ʱ��
		//strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
				
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = oper + "_" +  kv.get("START_LAC") + "_" + kv.get("START_SAC");
		String sLacCI = kv.get("START_LAC") + "_" + kv.get("START_SAC");
		
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		long lCellSysID = 0L;
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			lCellSysID = cell.getCellSysID();
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("START_SAC"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				lCellSysID = oper*10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("START_LAC")) * 100000L
						+ Long.parseLong(kv.get("START_SAC"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
				
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
		String belongCity = "0";
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
		//�Է���������ID
		sectionno = 0;
		str = Util.findByRegex(o_msisdn, "[0-9]*", 0);
		if(str!=null)
		{
			if(o_msisdn.length() >= 7)
			{
				sectionno = Integer.parseInt(String.valueOf(o_msisdn).substring(0,7));
			}
		}
		CFG_NUM_HOME home2 = NumHome.getInstance().getRegion(sectionno);
		if(home2 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}

		/*
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}*/
				
				//strLine.append(netid + subTemp.m_strNewFieldSplitSign);
		
		try {
				if(_monitor!=null)
				{
					//���
					String starttime = kv.get("START_TIME");
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					int callduration = kv.get("TALK_TIME")==null?0:Integer.parseInt(kv.get("TALK_TIME"));
					String cellname = cell==null?sLacCI:cell.getCellName();
					long cellsysid = lCellSysID;
					
					_monitor.IsTouchNet(msisdn, o_msisdn, formatter.parse(starttime), 
								cellname,cellsysid, mo_mt, callduration, oper*10+1);
					
					_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, o_msisdn);
					
					_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					String sLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
					int nLac = -1;
					if(sLac!=null)
						nLac = Integer.parseInt(kv.get("START_LAC"));
					
					_monitor.IsExitLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					_monitor.IsEnterLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					
					_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				}
				
				String lon = "null";
				String lat = "null";
				if(cell != null)
				{
					lon = String.valueOf(cell.getLon());
					lat = String.valueOf(cell.getLat());
				}
				
				//ʵʱ�û�����
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("��ؼ�¼�쳣",e);
			}
		
		if(subTemp.m_hasRowkey)
		{//����Hbase��Ҫ������rowkey�ֶ�
			//SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		
			String rowkey = String.format("%s_%s_%s_%s_%s_%d", 
					starttime,String.valueOf(imsi),msisdn,o_imsi,o_msisdn,lCellSysID);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}

		
		return strLine.toString();
	}
	
	public String parsercdr52(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		strLine.append(kv.get("SMS_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		String sSmsType = Util.findByRegex(kv.get("SMS_TYPE"), "[0-9]*", 0);
		int smstype = -1;
		if(sSmsType!=null)
			smstype = Integer.parseInt(kv.get("SMS_TYPE"));
		
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ĺ���
		//strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
		
		int servicetype = 0;
		String sServicetype = Util.findByRegex(kv.get("SERVICE_TYPE"), "[0-9]*", 0);
		if(sServicetype!=null)
			servicetype = Integer.parseInt(sServicetype);
		String msisdn = "-10000";
		if(servicetype == 6)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(servicetype == 7)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			if(msisdn!=null && msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn.equals("-10000"))
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
			
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//�Է�IMSI
		//�Է�IMEI
		//�Է�����
		String o_msisdn = "-10000";
		Long o_imsi = -10000L;
		if(servicetype == 6)
		{
			String strimsi = Util.findByRegex(kv.get("CALLED_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLED_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLED_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else if(servicetype == 7)
		{
			String strimsi = Util.findByRegex(kv.get("CALLING_IMSI"), "[0-9]*", 0);
			if(strimsi != null)
			{
				if(!kv.get("CALLING_IMSI").isEmpty())
					o_imsi = Long.valueOf(kv.get("CALLING_IMSI"));
			}
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		
		//ҵ�����LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����SAC
		strLine.append(kv.get("START_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//����ҵ��ģʽ
		//strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		//strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		//strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//���ų���
		//strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		//strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		
		//����Ѱ����Ӧʱ��
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�ʱ��
		//strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//���ŷ���/������ʱ��
		//strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
				
		//С��ϵͳ��
		String cellkey = oper + "_" +  kv.get("START_LAC") + "_" + kv.get("START_SAC");
		String sLacCI = kv.get("START_LAC") + "_" + kv.get("START_SAC");
		
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		long lCellSysID = 0L;
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			lCellSysID = cell.getCellSysID();
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("START_SAC"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				lCellSysID = oper*10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("START_LAC")) * 100000L
						+ Long.parseLong(kv.get("START_SAC"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
				
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
		String belongCity = "0";
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
		//�Է���������ID
		sectionno = 0;
		str = Util.findByRegex(o_msisdn, "[0-9]*", 0);
		if(str!=null)
		{
			if(o_msisdn.length() >= 7)
			{
				sectionno = Integer.parseInt(String.valueOf(o_msisdn).substring(0,7));
			}
		}
		CFG_NUM_HOME home2 = NumHome.getInstance().getRegion(sectionno);
		if(home2 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}


		try {
				if(_monitor!=null)
				{
					//���
					String starttime = kv.get("START_TIME");
					SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
					String cellname = cell==null?sLacCI:cell.getCellName();
					long cellsysid = lCellSysID;
				
					_monitor.IsTouchNet(msisdn, o_msisdn, formatter.parse(starttime), 
							cellname, cellsysid, smstype, 0, oper*10+2);
					
					_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, o_msisdn);
					
					_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
					
					String sLac = Util.findByRegex(kv.get("START_LAC"), "[0-9]*", 0);
					int nLac = -1;
					if(sLac!=null)
						nLac = Integer.parseInt(kv.get("START_LAC"));
					
					_monitor.IsExitLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					_monitor.IsEnterLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
					
					_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				}	
				
				String lon = "null";
				String lat = "null";
				if(cell != null)
				{
					lon = String.valueOf(cell.getLon());
					lat = String.valueOf(cell.getLat());
				}
				
				//ʵʱ�û�����
				
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("��ؼ�¼�쳣",e);
			}	
		
		if(subTemp.m_hasRowkey)
		{//����Hbase��Ҫ������rowkey�ֶ�
			//SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String rowkey = String.format("%s_%s_%s_%s_%s_%d", 
					starttime,String.valueOf(imsi),msisdn,o_imsi,o_msisdn,lCellSysID);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}
		
		return strLine.toString();
	}
	
	public String parsercdr53(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø�������
		strLine.append(kv.get("MM_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��¾ܾ�ԭ��
		//strLine.append(kv.get("LU_REJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN
		String msisdn = kv.get("MSISDN");
		if(msisdn.length() >= 11)
			msisdn = msisdn.substring(msisdn.length() - 11);
		
		Long imsi = -10000L;
		String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
		if(strimsi != null && !kv.get("IMSI").isEmpty())
		{
			imsi = Long.valueOf(kv.get("IMSI"));
		}
		if((msisdn.isEmpty() || msisdn.equals("-10000")) && imsi!=-10000L)
		{
			msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
		}
		
		
		
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI
		strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
		
		//IMEI
		strLine.append(kv.get("IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰCI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�CI
		strLine.append(kv.get("DEST_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		//strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		//strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//������Ӧʱ��
		//strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�ʱ��
		//strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
				
		//С��ϵͳ��
		String cellkey = oper + "_" +  kv.get("DEST_LAC") + "_" + kv.get("DEST_SAC");
		String sLacCI = kv.get("DEST_LAC") + "_" + kv.get("DEST_SAC");
		
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		long lCellSysID = 0L; 
				
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
			lCellSysID = cell.getCellSysID();
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("DEST_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("DEST_SAC"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				lCellSysID = oper*10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("DEST_LAC")) * 100000L
						+ Long.parseLong(kv.get("DEST_SAC"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
				
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
		String belongCity = "0";
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
				
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
			if(_monitor!=null)
			{
				//���
				String starttime = kv.get("START_TIME");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
				String cellname = cell==null?sLacCI:cell.getCellName();
				int mmtype = Integer.parseInt(kv.get("MM_TYPE"));
				long cellsysid = lCellSysID;
				
				_monitor.IsTouchNet(msisdn, "", formatter.parse(starttime), 
							cellname, cellsysid, mmtype, 0, oper*10+3);
				_monitor.IsPowerUp(msisdn, formatter.parse(starttime), cellname, cellsysid, mmtype);
				
				_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				String sLac = Util.findByRegex(kv.get("DEST_LAC"), "[0-9]*", 0);
				int nLac = -1;
				if(sLac!=null)
					nLac = Integer.parseInt(kv.get("DEST_LAC"));
				
				_monitor.IsExitLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				_monitor.IsEnterLAC(msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
				_monitor.BlongLacArea(belongCity, msisdn, formatter.parse(starttime), cellname, nLac, cellsysid);
			}
			
			
			String lon = "null";
			String lat = "null";
			if(cell != null)
			{
				lon = String.valueOf(cell.getLon());
				lat = String.valueOf(cell.getLat());
			}
			
			//ʵʱ�û�����
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("��ؼ�¼�쳣",e);
		}
		

		if(subTemp.m_hasRowkey)
		{//����Hbase��Ҫ������rowkey�ֶ�
			//SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String starttime = kv.get("START_TIME");
			try {
			SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			Date time;
			
				time = formatter1.parse(starttime);
			
			SimpleDateFormat formatter2 = new SimpleDateFormat("yyyyMMddHHmmss");
			starttime = formatter2.format(time);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			String rowkey = String.format("%s_%s_%s_%d", 
					starttime,String.valueOf(imsi),msisdn,lCellSysID);
			strLine = new StringBuffer(rowkey + subTemp.m_strNewFieldSplitSign + strLine.toString());
		}
		
		return strLine.toString();
	}

	/**
	 * ho
	 * @param subTemp
	 * @param kv
	 * @param cityID
	 * @return
	 */
	public String parserHO(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø�������
		int hotype = Integer.parseInt(kv.get("HO_TYPE"));
		hotype = hotype + 200;
		strLine.append(hotype + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��¾ܾ�ԭ��
		//strLine.append(kv.get("HO_DIR") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN
		String msisdn = kv.get("MSISDN");
		if(msisdn.length() >= 11)
			msisdn = msisdn.substring(msisdn.length() - 11);
				
		Long imsi = -10000L;
		if(msisdn.isEmpty() || msisdn.equals("-10000"))
		{
			String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
			if(strimsi != null && !kv.get("IMSI").isEmpty())
			{
				imsi = Long.valueOf(kv.get("IMSI"));
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
		}
		
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//IMEI
		strLine.append(kv.get("IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("SOUR_MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("SOUR_MNC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰCI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø������ʱ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = oper + "_" +  kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		String sLacCI = kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("DEST_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("DEST_CI"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				long lCellSysID = oper * 10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("DEST_LAC")) * 100000L
						+ Long.parseLong(kv.get("DEST_CI"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
		
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
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		
		try {
			/*
			if(_monitor!=null)
			{
				//���
				String starttime = kv.get("START_TIME");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
				String cellname = cell==null?"":cell.getCellName();
				int mmtype = Integer.parseInt(kv.get("MM_TYPE"));
				long cellsysid = cell==null?0:cell.getCellSysID();
				
				_monitor.IsTouchNet(msisdn, formatter.parse(starttime), 
							cellname, cellsysid, mmtype, 0, 2);
				_monitor.IsPowerUp(msisdn, formatter.parse(starttime), cellname, cellsysid, mmtype);
				
				_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
			}*/
			
			String lon = "null";
			String lat = "null";
			if(cell != null)
			{
				lon = String.valueOf(cell.getLon());
				lat = String.valueOf(cell.getLat());
			}
			
			//ʵʱ�û�����
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("��ؼ�¼�쳣",e);
		}
		return strLine.toString();
	}
	
	
	/**
	 * ho
	 * @param subTemp
	 * @param kv
	 * @param cityID
	 * @return
	 */
	public String parserRELOC(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø�������
		int hotype = Integer.parseInt(kv.get("HO_TYPE"));
		hotype = hotype + 200;
		strLine.append(hotype + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��¾ܾ�ԭ��
		//strLine.append(kv.get("HO_DIR") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN
		String msisdn = kv.get("MSISDN");
		if(msisdn.length() >= 11)
			msisdn = msisdn.substring(msisdn.length() - 11);
				
		Long imsi = -10000L;
		if(msisdn.isEmpty() || msisdn.equals("-10000"))
		{
			String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
			if(strimsi != null && !kv.get("IMSI").isEmpty())
			{
				imsi = Long.valueOf(kv.get("IMSI"));
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//��������
			}
		}
		
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//IMEI
		strLine.append(kv.get("IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("SOUR_MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("SOUR_MNC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰCI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø������ʱ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = oper + "_" +  kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		String sLacCI = kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//��Ӫ�̱��*10000000000000+��������*10000000000+LAC*100000+CI
			String strLac = Util.findByRegex(kv.get("DEST_LAC"), "[0-9]*", 0);
			String strCI = Util.findByRegex(kv.get("DEST_CI"), "[0-9]*", 0);
			
			if(strLac!=null && strCI!=null)
			{
				long lCellSysID = oper * 10000000000000L 
						+ (long)cityID * 10000000000L 
						+ Long.parseLong(kv.get("DEST_LAC")) * 100000L
						+ Long.parseLong(kv.get("DEST_CI"));
				strLine.append(lCellSysID + subTemp.m_strNewFieldSplitSign);
			}
			else
			{
				strLine.append("" + subTemp.m_strNewFieldSplitSign);
			}
		}
		
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
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//С������
		if(cell!=null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		
		try {
			/*
			if(_monitor!=null)
			{
				//���
				String starttime = kv.get("START_TIME");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
				String cellname = cell==null?"":cell.getCellName();
				int mmtype = Integer.parseInt(kv.get("MM_TYPE"));
				long cellsysid = cell==null?0:cell.getCellSysID();
				
				_monitor.IsTouchNet(msisdn, formatter.parse(starttime), 
							cellname, cellsysid, mmtype, 0, 2);
				_monitor.IsPowerUp(msisdn, formatter.parse(starttime), cellname, cellsysid, mmtype);
				
				_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
			}*/
			
			String lon = "null";
			String lat = "null";
			if(cell != null)
			{
				lon = String.valueOf(cell.getLon());
				lat = String.valueOf(cell.getLat());
			}
			
			//ʵʱ�û�����
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("��ؼ�¼�쳣",e);
		}
		return strLine.toString();
	}
	
	
	/**
	 * ���ͼ��socket��Ϣ
	 * @param imsi
	 * @param msisdn
	 * @param data
	 */
	private void SocketMonitor(String imsi,String msisdn,
			String data)
	{
		String strimsi = imsi;
		HashMap<String,List<Integer>> imsiMap = MessageQueue.getInstance().GetImsiQueue();
		if(imsiMap.containsKey(strimsi))
		{
			SendResult send = new SendResult();
			send.SendData(imsiMap.get(strimsi), data);
		}
		
		HashMap<String,List<Integer>> msisdnMap = MessageQueue.getInstance().GetMsisdnQueue();
		if(msisdnMap.containsKey(msisdn))
		{
			SendResult send = new SendResult();
			send.SendData(msisdnMap.get(msisdn), data);
		}
	}
	
}
