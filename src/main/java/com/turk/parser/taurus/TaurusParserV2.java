package com.turk.parser.taurus;

import java.text.SimpleDateFormat;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.parser.Parser;
import com.turk.parser.taurus.model.CFG_NUM_HOME;
import com.turk.parser.taurus.model.MOD_CELL;

import com.turk.task.CollectObjInfo;
import com.turk.templet.LineTempletP;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class TaurusParserV2 {
	private MonitorHit _monitor;
	private CollectObjInfo collectObjInfo;
	private Logger log = LogMgr.getInstance().getSystemLogger();

	public TaurusParserV2(MonitorHit monitor, Parser objParser)
	{
		_monitor = monitor;
		collectObjInfo = objParser.getCollectObjInfo();
	}
	
	
	public String parserCC(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//EndTime
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//cdr_id
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSC	bsc
		strLine.append(kv.get("OPC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC	msc
		strLine.append(kv.get("DPC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP	mgw_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP	msc_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//service_type
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��������	mo_mt	"��call_type����
		int mo_mt = Integer.parseInt(kv.get("CALL_TYPE"));
		String sCalltype = "";
		//mo_mt��case mo_mt when 0 then '����' when 1 then '����' when 2 then '�л�' when 3 then '��������' else '����' end as sub_type��
		switch(mo_mt)
		{
			case 0:
				sCalltype = "����";
				break;
			case 1:
				sCalltype = "����";
				break;
			case 2:
				sCalltype = "�л�";
				break;
			case 3:
				sCalltype = "��������";
				break;
			default:
				sCalltype = "����";
				break;
		}
		//��call_type����
		strLine.append(sCalltype + subTemp.m_strNewFieldSplitSign);
		
		//result
		strLine.append(kv.get("A_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//��֪��������	drop_type
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI	imsi	bigint	��IMSI��
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//����IMEI	imei
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
				
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
		
		//�Է�IMSI	o_imsi
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//�Է�IMEI	o_imei
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		
		//�Է�����	mt_msisdn	character varying(32)	"����������Ϊ0�������к��롿 ����������Ϊ1�������к��롿
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0�������к��롿
			//������ʽ���ж��Ƿ�Ϊ����
			String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{//1�������к��롿
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}

		
		//���Ӻ���	connected_num
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//ǰת����	redirect_num
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��������	other_num
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI	first_tmsi
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI	last_tmsi
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//MCC	mcc
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MNC	mnc
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ֹ��LAC	end_lac	integer	end_lac
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
				
		//ҵ����ֹ��CI	end_ci	integer	end_ci
		strLine.append(kv.get("END_CI") + subTemp.m_strNewFieldSplitSign);
		
		//ԴLAC	sour_lac	integer	sour_lac
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
				
		//ԴCI	sour_ci	integer	sour_ci
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
				
		//Ŀ��LAC	dest_lac	integer	dest_lac
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
				
		//Ŀ��CI	dest_ci	integer	dest_ci
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
				
		//Ѱ����־	pr_flag
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��·ʱ϶��TS	cic5
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��·ʶ����PCM	cic7
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л�����	ho_type
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л�ԭ��	ho_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л����	ho_result
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���1	ho_ref1
		strLine.append(kv.get("HO_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���2	ho_ref2
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л��ܾ�ԭ��	horeject_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л�ʧ��ԭ��	hofailure_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����ԭ��	discon_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����������	discon_direct
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		

		//�������ԭ��	clearrequest_cause
		strLine.append(kv.get("CLEAR_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��	clearcommand_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��	usrpr_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����Ѱ����Ӧʱ��	netpr_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��	setup_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��	alert_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//���ӣ�Ӧ��ʱ��	conn_delay
		strLine.append(kv.get("CONN_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��	discon_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��	clear_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�����Ÿ���	sm_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//���б��ִ���	hold_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//���лָ�����	retrieve_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//DTMF��������	dtmf_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//DTMF�ܾ�����	dtmfrefuse_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//ͨ��ʱ��	talk_time
		strLine.append(kv.get("CLEAR_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//TCHռ��ʱ��	seize_time
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����ID	city_id	smallint	����ID�����ݲɼ��ĳ�����д���������ǣ�531
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);
				
		
		//����CI	ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�
		int ci = Integer.parseInt(kv.get("S_IP"));
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(ci);
				
		//С��ϵͳ��	cell_sys_id	bigint	��s_ip����ѯmod_cell��ipint��Ӧ��cell_sys_id�ֶ�
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}

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
			//�Է���������ID	o_city_id	smallint	ͨ���������ĶԷ��绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		if(cell != null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//����ID	net_id
		strLine.append("3" + subTemp.m_strNewFieldSplitSign);
		
		
		try {
			
		//���
		String starttime = kv.get("START_TIME");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
		String cellname = cell==null?"":cell.getCellName();
		long cellsysid = cell==null?0:cell.getCellSysID();
		_monitor.IsTouchNet(msisdn, "", formatter.parse(starttime), 
					cellname,cellsysid, mo_mt, callduration, 0);
		_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, mt_msisdn);
		
		_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
		
		_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("��ؼ�¼�쳣",e);
		}
		
		return strLine.toString();
	}
	
	public String parserSM(LineTempletP.SubTemplet subTemp,Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��	end_time	timestamp without time zone	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��	cdr_id	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//bsc	bsc	integer	�����ɼ��ֶΣ�opc
		strLine.append(kv.get("OPC") + subTemp.m_strNewFieldSplitSign);
		
		//msc	msc	integer	�����ɼ��ֶΣ�dpc
		strLine.append(kv.get("DPC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP	mgw_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP	msc_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������	service_type	smallint	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��������	sms_type	character varying(16)	
		//mo_mt ��case mo_mt when 0 then '���ŷ���' when 1 then '���Ž���'
		//else '����' end as sub_type��
		int mo_mt = Integer.parseInt(kv.get("SMS_TYPE"));
		String sSMSType = "";
		switch(mo_mt)
		{
			case 0:
				sSMSType = "���ŷ���";
				break;
			case 1:
				sSMSType = "���Ž���";
				break;
			default:
				sSMSType = "����";
				break;
		}
		
		//mo_mt	smallint	"��sms_type����0�����ŷ���MO��1�����Ž���MT��"
		strLine.append(sSMSType + subTemp.m_strNewFieldSplitSign);
		
		
		//ҵ����ϸ���	result	smallint	result
		strLine.append(kv.get("SMS_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ĺ���	smsc	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI	imsi	bigint	imsi
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//����IMEI	imei	character varying(32)	esn
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//��������	msisdn	character varying(32)	msisdn
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
		
		//�Է�IMSI	o_imsi	bigint	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�Է�IMEI	o_imei	character varying(32)	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
				
		//�Է�����	o_msisdn	character varying(32)	o_msisdn
		//o_msisdn	character varying(32)	"����������Ϊ0�������к��롿
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0�������к��롿
			//������ʽ���ж��Ƿ�Ϊ����
			String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
			
		}
		else if(mo_mt == 1)
		{//1�������к��롿
			String str = Util.findByRegex(kv.get("CALLING"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}
		
		//��ʼTMSI	first_tmsi	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI	last_tmsi	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MCC	mcc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MNC	mnc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC	start_lac	smallint	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI	start_ci	smallint	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־	pr_flag	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����ҵ��ģʽ	sms_mode	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��	clearrequest_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��	clearcommand_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//���ų���	sms_lenth	smallint	sms_lenth
		strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��	usrpr_delay	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//����Ѱ����Ӧʱ��	netpr_delay	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��	clear_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//���ŷ���/������ʱ��	sms_delay	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//����ID	city_id	smallint	����ID�����ݲɼ��ĳ�����д���������ǣ�531
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);

		//����CI	ci	character varying(32)	��s_ip����ѯal_cellid���Ӧ�Ļ�վ���ƣ�����ƥ���ÿ�
		int ci = Integer.parseInt(kv.get("S_IP"));
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(ci);

		
		//С��ϵͳ��	cell_sys_id	bigint	��s_ip����ѯmod_cell��ipint��Ӧ��cell_sys_id�ֶ�
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
				
		int sectionno = 0;
		if(msisdn.length() >= 7)
		{
			sectionno = Integer.parseInt(String.valueOf(msisdn).substring(0,7));
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
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}

		if(cell != null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//����������Ӫ��	operator	character varying(32)	��ʱΪ��
				strLine.append("3" + subTemp.m_strNewFieldSplitSign);
		
		try {
			
			//���
			String starttime = kv.get("START_TIME");
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String cellname = cell==null?"":cell.getCellName();
			long cellsysid = cell==null?0:cell.getCellSysID();
			
				_monitor.IsTouchNet(msisdn, "", formatter.parse(starttime), 
						cellname, cellsysid, mo_mt, 0, 1);
				
				_monitor.IsCallNum(msisdn, formatter.parse(starttime), cellname, cellsysid, mt_msisdn);
				
				_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("��ؼ�¼�쳣",e);
			}
			
		return strLine.toString();
	}
	
	public String parserMM(LineTempletP.SubTemplet subTemp,Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��	end_time	timestamp without time zone	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��	cdr_id	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSC	bsc	integer	�����ɼ��ֶΣ�opc
		strLine.append(kv.get("OPC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC	msc	integer	�����ɼ��ֶΣ�dpc
		strLine.append(kv.get("DPC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP	mgw_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP	msc_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������	service_type	smallint	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		
		//λ�ø�������	mm_type	character varying(16)	
		//reg_type��case reg_type when 0 then '����λ�ø���' 
		//when 1 then '����' when 2 then 'LACλ�ø���' when 3 then '�ػ�' 
		//else '����' end as sub_type��

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
		int nMMType = Integer.parseInt(kv.get("MM_TYPE"));
		String sMMType = "";
		switch(nMMType)
		{
			case 0:
				sMMType = "����λ�ø���";
				break;
			case 1:
				sMMType = "����";
				break;
			case 2:
				sMMType = "LACλ�ø���";
				break;
			case 3:
				sMMType = "�ػ�";
				break;
			default:
				sMMType = "����";
				break;
		}
		strLine.append(sMMType + subTemp.m_strNewFieldSplitSign);
		
		//mm_result	smallint	mm_result
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
				
		//λ�ø��¾ܾ�ԭ��	lu_reject_cause	smallint	
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN	msisdn		msisdn
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
				
				
		//IMSI	imsi	bigint	imsi
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//IMEI	imei	character varying(32)	esn
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI	first_tmsi	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI	last_tmsi	integer	tmsi_d
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//MCC	mcc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MNC	mnc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC	sour_lac	smallint	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰCI	sour_ci	smallint	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�LAC	dest_lac	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�CI	dest_ci	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��	clearrequest_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��	clearcommand_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø������ʱ��	resp_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��	clear_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//����ID	city_id	smallint	����ID�����ݲɼ��ĳ�����д���������ǣ�531
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);
				
		//С��ϵͳ��	cell_sys_id	bigint	��s_ip����ѯmod_cell��ipint��Ӧ��cell_sys_id�ֶ�
		int ci = Integer.parseInt(kv.get("S_IP"));
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(ci);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
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
		if(home1 != null)
		{
			//������������ID	s_city_id	smallint	ͨ���������ı����绰���룬��ͨ����������ر�cfg_num_home��������������ĵ���ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		if(cell != null)
		{
			strLine.append(cell.getCellName() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		

		//��Ӫ�̱�ʶ	op_id	smallint	�ƶ�1����ͨ2������3
		strLine.append("3" + subTemp.m_strNewFieldSplitSign);
		
		
		try {
			
				//���
				String starttime = kv.get("START_TIME");
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				//int callduration = Integer.parseInt(kv.get("CLEAR_TIME"));
				String cellname = cell==null?"":cell.getCellName();
				int mmtype = Integer.parseInt(kv.get("MM_TYPE"));
				long cellsysid = cell==null?0:cell.getCellSysID();
				
				_monitor.IsTouchNet(msisdn, "", formatter.parse(starttime), 
							cellname, cellsysid, mmtype, 0, 2);
				_monitor.IsPowerUp(msisdn, formatter.parse(starttime), cellname, cellsysid, mmtype);
				
				_monitor.IsExitArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
				_monitor.IsEnterArea(msisdn, formatter.parse(starttime), cellname, cellsysid);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				log.error("��ؼ�¼�쳣",e);
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
			Map<String,String> kv,int cityID,int netid)
	{
		StringBuffer strLine = new StringBuffer();
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//��������call_type��case call_type when 0 then '����' when 1 then '����'
		//when 2 then '�л�' when 3 then '��������' when 4 then 'ҵ���ؽ�' 
		//when 5 then '��������' when 6 then '��������'
		//else '����' end as sub_type��
		int mo_mt = Integer.parseInt(kv.get("CALL_TYPE"));
		String sCalltype = "";
		switch(mo_mt)
		{
			case 0:
				sCalltype = "����";
				break;
			case 1:
				sCalltype = "����";
				break;
			case 2:
				sCalltype = "�л�";
				break;
			case 3:
				sCalltype = "��������";
				break;
			case 4:
				sCalltype = "ҵ���ؽ�";
				break;
			case 5:
				sCalltype = "��������";
				break;
			case 6:
				sCalltype = "��������";
				break;
			default:
				sCalltype = "����";
				break;
		}
		strLine.append(sCalltype + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//��֪��������
		strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
		
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
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
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
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//���Ӻ���
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//ǰת����
		strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
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
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//��·ʱ϶��TS
		strLine.append(kv.get("CIC5") + subTemp.m_strNewFieldSplitSign);
		
		//��·ʶ����PCM
		strLine.append(kv.get("CIC7") + subTemp.m_strNewFieldSplitSign);
		
		//�л�����
		strLine.append(kv.get("HO_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//�л�ԭ��
		strLine.append(kv.get("HO_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�л����
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���1
		strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���2
		strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ܾ�ԭ��
		strLine.append(kv.get("HOREJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�л�ʧ��ԭ��
		strLine.append(kv.get("HOFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//����ԭ��
		strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//����������
		strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����Ѱ����Ӧʱ��
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//���ӣ�Ӧ��ʱ��
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//�����Ÿ���
		strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//���б��ִ���
		strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//���лָ�����
		strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF��������
		strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF�ܾ�����
		strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//ͨ��ʱ��
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//TCHռ��ʱ��
		strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = kv.get("START_LAC") + "_" + kv.get("START_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
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
		
		//�Է���������ID
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
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}

		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		strLine.append(netid + subTemp.m_strNewFieldSplitSign);
		
		return strLine.toString();
	}
	
	/**
	 * UNCIOM SM
	 * @param subTemp
	 * @param kv
	 * @return
	 */
	public String parsercdr22(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int netid)
	{
		StringBuffer strLine = new StringBuffer();
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//��������
        strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
				
		//ҵ������
		//sms_type ��case sms_type when 0 then '���ŷ���' when 1 then '���Ž���' 
		//when 2 then '�����ύ����' when 3 then '�����·�����' when 4 then '����״̬����' 
		//when 5 then 'WAP PUSH���Ž���' when 6 then '��������' else '����' end as sub_type��
		int servicetype = Integer.parseInt(kv.get("SMS_TYPE"));
		String sServicetype = "";
		switch(servicetype)
		{
			case 0:
				sServicetype = "���ŷ���";
				break;
			case 1:
				sServicetype = "���Ž���";
				break;
			case 2:
				sServicetype = "�����ύ����";
				break;
			case 3:
				sServicetype = "�����·�����";
				break;
			case 4:
				sServicetype = "����״̬����";
				break;
			case 5:
				sServicetype = "WAP PUSH���Ž���";
				break;
			case 6:
				sServicetype = "��������";
				break;
			default:
				sServicetype = "����";
				break;
		}
		
		strLine.append(sServicetype + subTemp.m_strNewFieldSplitSign);
		
		
		
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ĺ���
		strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
		
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
			
			if(msisdn.isEmpty() || msisdn == "-10000")
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
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//����ҵ��ģʽ
		strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//���ų���
		strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����Ѱ����Ӧʱ��
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//���ŷ���/������ʱ��
		strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = kv.get("START_LAC") + "_" + kv.get("START_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
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
		
		//�Է���������ID
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
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}

		strLine.append(netid + subTemp.m_strNewFieldSplitSign);
		
		return strLine.toString();
	}
		
	
	public String parsercdr23(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int netid)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø�������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		//ҵ������
		//mm_type ��case mm_type when 0 then 'LACλ�ø���' when 1 then '����λ�ø���' 
		//when 2 then '����' when 3 then '�ػ�' else '����' end as sub_type��
		int nMMType = Integer.parseInt(kv.get("MM_TYPE"));
		String sMMType = "";
		
		switch(nMMType)
		{
			case 0:
				sMMType = "LACλ�ø���";
				break;
			case 1:
				sMMType = "����λ�ø���";
				break;
			case 2:
				sMMType = "����";
				break;
			case 3:
				sMMType = "�ػ�";
				break;
			default:
				sMMType = "����";
				break;
		}
		
		strLine.append(sMMType + subTemp.m_strNewFieldSplitSign);
		
		
		
		//ҵ����ϸ���
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��¾ܾ�ԭ��
		strLine.append(kv.get("LU_REJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN
		String msisdn = kv.get("MSISDN");
		if(msisdn.length() >= 11)
			msisdn = msisdn.substring(msisdn.length() - 11);
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
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰCI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��
		strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø������ʱ��
		strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��
		strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
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
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		strLine.append(netid + subTemp.m_strNewFieldSplitSign);

		return strLine.toString();
	}
	
	public String parsercdr24(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int netid)
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
		//service_type��case result when 0 then '����Ѱ��' when 1 then '����Ѱ��' 
		//when 2 then 'λ�ø���Ѱ��' 
		//when 3 then '����ҵ��Ѱ��' else '����' end as sub_type��
		int nServicetype = Integer.parseInt(kv.get("SERVICE_TYPE"));
		String sServicetype = "";
		
		switch(nServicetype)
		{
			case 0:
				sServicetype = "����Ѱ��";
				break;
			case 1:
				sServicetype = "����Ѱ��";
				break;
			case 2:
				sServicetype = "λ�ø���Ѱ��";
				break;
			case 3:
				sServicetype = "����ҵ��Ѱ��";
				break;
			default:
				sServicetype = "����";
				break;
		}
		
		strLine.append(sServicetype + subTemp.m_strNewFieldSplitSign);
		
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
		String cellkey = kv.get("START_LAC") + "_" + kv.get("START_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
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

		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		strLine.append(netid + subTemp.m_strNewFieldSplitSign);
		
		return strLine.toString();
	}
	
	public String parsercdr51(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int netid)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		//call_type ��case call_type when 0 then '����' when 1 then '����' 
		//when 2 then '�л�' when 3 then '��������' when 4 then 'ҵ���ؽ�' 
		//when 5 then '��������' when 6 then '��������' else '����' end as sub_type��

		int mo_mt = Integer.parseInt(kv.get("CALL_TYPE"));
		String sCalltype = "";
		switch(mo_mt)
		{
			case 0:
				sCalltype = "����";
				break;
			case 1:
				sCalltype = "����";
				break;
			case 2:
				sCalltype = "�л�";
				break;
			case 3:
				sCalltype = "��������";
				break;
			case 4:
				sCalltype = "ҵ���ؽ�";
				break;
			case 5:
				sCalltype = "��������";
				break;
			case 6:
				sCalltype = "��������";
				break;
			default:
				sCalltype = "����";
				break;
		}
		strLine.append(sCalltype + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//��֪��������
		strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
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
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
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
			
			if(msisdn.isEmpty() || msisdn == "-10000")
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//���Ӻ���
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//ǰת����
		strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
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
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//��·ʱ϶��TS	cic5	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//��·ʶ����PCM	cic7	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л�����
		strLine.append(kv.get("INFO_TRANS_CAP") + subTemp.m_strNewFieldSplitSign);
				
		//�л�ԭ��
		strLine.append(kv.get("RELOC_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//�л����
		strLine.append(kv.get("RELOC_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���1
		strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ο���2
		strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
		
		//�л��ܾ�ԭ��	horeject_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�л�ʧ��ԭ��
		strLine.append(kv.get("RELOCFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//����ԭ��
		strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//����������
		strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ԭ��	clearrequest_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		//�������ԭ��	clearcommand_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//����Ѱ����Ӧʱ��
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//����ʱ��
		strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//����ʱ��
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//���ӣ�Ӧ��ʱ��
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//����ʱ��
		strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP���ʱ��	clear_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//�����Ÿ���
		strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//���б��ִ���
		strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//���лָ�����
		strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//DTMF��������
		strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//DTMF�ܾ�����
		strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		
		//ͨ��ʱ��
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//ռ��ʱ��
		strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = kv.get("START_LAC") + "_" + kv.get("START_SAC");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
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
		
		//�Է���������ID
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
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		strLine.append(netid + subTemp.m_strNewFieldSplitSign);
		
		return strLine.toString();
	}
	
	public String parsercdr52(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int netid)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//��������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
				
		//ҵ������
		//sms_type ��case sms_type when 0 then '���ŷ���' when 1 then '���Ž���' 
		//when 2 then '�����ύ����' when 3 then '�����·�����' when 4 then '����״̬����' 
		//when 5 then 'WAP PUSH���Ž���' when 6 then '��������' else '����' end as sub_type��
		int servicetype = Integer.parseInt(kv.get("SMS_TYPE"));
		String sServicetype = "";
		switch(servicetype)
		{
			case 0:
				sServicetype = "���ŷ���";
				break;
			case 1:
				sServicetype = "���Ž���";
				break;
			case 2:
				sServicetype = "�����ύ����";
				break;
			case 3:
				sServicetype = "�����·�����";
				break;
			case 4:
				sServicetype = "����״̬����";
				break;
			case 5:
				sServicetype = "WAP PUSH���Ž���";
				break;
			case 6:
				sServicetype = "��������";
				break;
			default:
				sServicetype = "����";
				break;
		}
				
		strLine.append(sServicetype + subTemp.m_strNewFieldSplitSign);
		
		
		
		//ҵ����ϸ���
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//�������ĺ���
		strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//����IMSI
		//����IMEI
		//��������
		Long imsi = -10000L;
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
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
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
			
			if(msisdn.isEmpty() || msisdn == "-10000")
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
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
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//��Ϊmsisdn����ĳ���Ϊ13λ������86��ͷ�����86ȥ����
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//��ʼTMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//��ֹTMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		
		//ҵ�����LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//ҵ�����SAC
		strLine.append(kv.get("START_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Ѱ����־
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//����ҵ��ģʽ
		strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//���ų���
		strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//�û�Ѱ����Ӧʱ��
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		
		//����Ѱ����Ӧʱ��
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�ʱ��
		strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//���ŷ���/������ʱ��
		strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = kv.get("START_LAC") + "_" + kv.get("START_SAC");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
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
		
		//�Է���������ID
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
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}

		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		strLine.append(netid + subTemp.m_strNewFieldSplitSign);
		return strLine.toString();
	}
	
	public String parsercdr53(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int netid)
	{
		StringBuffer strLine = new StringBuffer();
		
		//��ʼʱ��
		//��ʼʱ��
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//����ʱ��
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR��ʶ��
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø�������
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		//ҵ������
		//mm_type ��case mm_type when 0 then 'LACλ�ø���' when 1 then '����λ�ø���' 
		//when 2 then '����' when 3 then '�ػ�' else '����' end as sub_type��
		int nMMType = Integer.parseInt(kv.get("MM_TYPE"));
		String sMMType = "";
				
		switch(nMMType)
		{
			case 0:
				sMMType = "LACλ�ø���";
				break;
			case 1:
				sMMType = "����λ�ø���";
				break;
			case 2:
				sMMType = "����";
				break;
			case 3:
				sMMType = "�ػ�";
				break;
			default:
				sMMType = "����";
				break;
		}
				
		strLine.append(sMMType + subTemp.m_strNewFieldSplitSign);
		
		//ҵ����ϸ���
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��¾ܾ�ԭ��
		strLine.append(kv.get("LU_REJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN
		String msisdn = kv.get("MSISDN");
		if(msisdn.length() >= 11)
			msisdn = msisdn.substring(msisdn.length() - 11);
		
		Long imsi = -10000L;
		if(msisdn.isEmpty() || msisdn == "-10000")
		{
			String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
			if(strimsi != null)
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
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰLAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø���ǰCI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//λ�ø��º�CI
		strLine.append(kv.get("DEST_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�����ԭ��
		strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//������Ӧʱ��
		strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu�ͷ�ʱ��
		strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		
		//����ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//С��ϵͳ��
		String cellkey = kv.get("DEST_LAC") + "_" + kv.get("DEST_SAC");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
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
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		if(cell != null)
		{
			strLine.append(cell.getLon() + subTemp.m_strNewFieldSplitSign);
			strLine.append(cell.getLat() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}

		strLine.append(netid + subTemp.m_strNewFieldSplitSign);
		
		return strLine.toString();
	}
}
