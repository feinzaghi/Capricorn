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
		
		//动作类型	mo_mt	"【call_type】，
		int mo_mt = Integer.parseInt(kv.get("CALL_TYPE"));
		String sCalltype = "";
		//mo_mt（case mo_mt when 0 then '主叫' when 1 then '被叫' when 2 then '切换' when 3 then '紧急呼叫' else '其他' end as sub_type）
		switch(mo_mt)
		{
			case 0:
				sCalltype = "主叫";
				break;
			case 1:
				sCalltype = "被叫";
				break;
			case 2:
				sCalltype = "切换";
				break;
			case 3:
				sCalltype = "紧急呼叫";
				break;
			default:
				sCalltype = "其他";
				break;
		}
		//【call_type】，
		strLine.append(sCalltype + subTemp.m_strNewFieldSplitSign);
		
		//result
		strLine.append(kv.get("A_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//感知掉话类型	drop_type
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI	imsi	bigint	【IMSI】
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMEI	imei
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
				
		//本方号码msisdn	character varying(32)	通过【IMSI】和map_imsi_msisdn表查询出对应的电话号码
		Long imsi = -10000L;
		String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
		if(strimsi != null)
		{
			if(!kv.get("IMSI").isEmpty())
				imsi = Long.valueOf(kv.get("IMSI"));
		}
		
		String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//对方IMSI	o_imsi
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//对方IMEI	o_imei
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		
		//对方号码	mt_msisdn	character varying(32)	"当动作类型为0：【被叫号码】 当动作类型为1：【主叫号码】
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0：【被叫号码】
			//正则表达式，判断是否为数字
			String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{//1：【主叫号码】
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}

		
		//连接号码	connected_num
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//前转号码	redirect_num
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//其他号码	other_num
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI	first_tmsi
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI	last_tmsi
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//MCC	mcc
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MNC	mnc
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//业务终止的LAC	end_lac	integer	end_lac
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
				
		//业务终止的CI	end_ci	integer	end_ci
		strLine.append(kv.get("END_CI") + subTemp.m_strNewFieldSplitSign);
		
		//源LAC	sour_lac	integer	sour_lac
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
				
		//源CI	sour_ci	integer	sour_ci
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
				
		//目标LAC	dest_lac	integer	dest_lac
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
				
		//目标CI	dest_ci	integer	dest_ci
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
				
		//寻呼标志	pr_flag
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//电路时隙号TS	cic5
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//电路识别码PCM	cic7
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换类型	ho_type
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换原因	ho_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换结果	ho_result
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号1	ho_ref1
		strLine.append(kv.get("HO_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号2	ho_ref2
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换拒绝原因	horeject_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换失败原因	hofailure_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//断连原因	discon_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//断连发起方向	discon_direct
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		

		//清除请求原因	clearrequest_cause
		strLine.append(kv.get("CLEAR_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因	clearcommand_cause
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延	usrpr_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//网络寻呼响应时延	netpr_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//建立时延	setup_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//振铃时延	alert_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//连接（应答）时延	conn_delay
		strLine.append(kv.get("CONN_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//断连时延	discon_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延	clear_delay
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//含短信个数	sm_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//呼叫保持次数	hold_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//呼叫恢复次数	retrieve_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//DTMF启动次数	dtmf_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//DTMF拒绝次数	dtmfrefuse_count
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//通话时长	talk_time
		strLine.append(kv.get("CLEAR_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//TCH占用时长	seize_time
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//城市ID	city_id	smallint	城市ID，根据采集的城市填写，济南是是：531
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);
				
		
		//阿朗CI	ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空
		int ci = Integer.parseInt(kv.get("S_IP"));
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(ci);
				
		//小区系统号	cell_sys_id	bigint	【s_ip】查询mod_cell找ipint对应的cell_sys_id字段
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
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
			//对方归属城市ID	o_city_id	smallint	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
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
		
		//网络ID	net_id
		strLine.append("3" + subTemp.m_strNewFieldSplitSign);
		
		
		try {
			
		//监控
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
			log.error("监控记录异常",e);
		}
		
		return strLine.toString();
	}
	
	public String parserSM(LineTempletP.SubTemplet subTemp,Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间	end_time	timestamp without time zone	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号	cdr_id	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//bsc	bsc	integer	新增采集字段：opc
		strLine.append(kv.get("OPC") + subTemp.m_strNewFieldSplitSign);
		
		//msc	msc	integer	新增采集字段：dpc
		strLine.append(kv.get("DPC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP	mgw_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP	msc_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型	service_type	smallint	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//短信类型	sms_type	character varying(16)	
		//mo_mt （case mo_mt when 0 then '短信发送' when 1 then '短信接收'
		//else '其他' end as sub_type）
		int mo_mt = Integer.parseInt(kv.get("SMS_TYPE"));
		String sSMSType = "";
		switch(mo_mt)
		{
			case 0:
				sSMSType = "短信发送";
				break;
			case 1:
				sSMSType = "短信接收";
				break;
			default:
				sSMSType = "其他";
				break;
		}
		
		//mo_mt	smallint	"【sms_type】，0：短信发送MO；1：短信接受MT。"
		strLine.append(sSMSType + subTemp.m_strNewFieldSplitSign);
		
		
		//业务详细结果	result	smallint	result
		strLine.append(kv.get("SMS_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//短信中心号码	smsc	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI	imsi	bigint	imsi
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMEI	imei	character varying(32)	esn
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//本方号码	msisdn	character varying(32)	msisdn
		//msisdn	character varying(32)	通过【IMSI】和map_imsi_msisdn表查询出对应的电话号码
		Long imsi = -10000L;
		String strimsi = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
		if(strimsi != null)
		{
			if(!kv.get("IMSI").isEmpty())
				imsi = Long.valueOf(kv.get("IMSI"));
		}
		
		String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//对方IMSI	o_imsi	bigint	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//对方IMEI	o_imei	character varying(32)	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
				
		//对方号码	o_msisdn	character varying(32)	o_msisdn
		//o_msisdn	character varying(32)	"当动作类型为0：【被叫号码】
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0：【被叫号码】
			//正则表达式，判断是否为数字
			String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
			
		}
		else if(mo_mt == 1)
		{//1：【主叫号码】
			String str = Util.findByRegex(kv.get("CALLING"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}
		
		//初始TMSI	first_tmsi	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI	last_tmsi	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MCC	mcc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MNC	mnc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC	start_lac	smallint	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI	start_ci	smallint	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志	pr_flag	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//短信业务模式	sms_mode	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因	clearrequest_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因	clearcommand_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//短信长度	sms_lenth	smallint	sms_lenth
		strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延	usrpr_delay	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//网络寻呼响应时延	netpr_delay	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延	clear_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//短信发送/接收总时延	sms_delay	integer	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//城市ID	city_id	smallint	城市ID，根据采集的城市填写，济南是是：531
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);

		//阿朗CI	ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空
		int ci = Integer.parseInt(kv.get("S_IP"));
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(ci);

		
		//小区系统号	cell_sys_id	bigint	【s_ip】查询mod_cell找ipint对应的cell_sys_id字段
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
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
		
		//本方归属运营商	operator	character varying(32)	暂时为空
				strLine.append("3" + subTemp.m_strNewFieldSplitSign);
		
		try {
			
			//监控
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
				log.error("监控记录异常",e);
			}
			
		return strLine.toString();
	}
	
	public String parserMM(LineTempletP.SubTemplet subTemp,Map<String,String> kv)
	{
		StringBuffer strLine = new StringBuffer();
		//StartTime
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间	end_time	timestamp without time zone	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号	cdr_id	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSC	bsc	integer	新增采集字段：opc
		strLine.append(kv.get("OPC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC	msc	integer	新增采集字段：dpc
		strLine.append(kv.get("DPC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP	mgw_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP	msc_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型	service_type	smallint	
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		
		//位置更新类型	mm_type	character varying(16)	
		//reg_type（case reg_type when 0 then '周期位置更新' 
		//when 1 then '开机' when 2 then 'LAC位置更新' when 3 then '关机' 
		//else '其他' end as sub_type）

		//reg_type	smallint	"【mm_type】，
		//0 基于时间
		//1 开机
		//2 基于区域
		//3 关机
		//4 参数更改
		///5 基于指令
		//6 基于距离
		//7 基于用户区
		//9 BCMC登记"	【mm_type】
		int nMMType = Integer.parseInt(kv.get("MM_TYPE"));
		String sMMType = "";
		switch(nMMType)
		{
			case 0:
				sMMType = "周期位置更新";
				break;
			case 1:
				sMMType = "开机";
				break;
			case 2:
				sMMType = "LAC位置更新";
				break;
			case 3:
				sMMType = "关机";
				break;
			default:
				sMMType = "其他";
				break;
		}
		strLine.append(sMMType + subTemp.m_strNewFieldSplitSign);
		
		//mm_result	smallint	mm_result
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
				
		//位置更新拒绝原因	lu_reject_cause	smallint	
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//MSISDN	msisdn		msisdn
		//msisdn	character varying(32)	通过IMSI】和map_imsi_msisdn表查询出对应的电话号码	【mdn】和map_imsi_msisdn表查询出对应的电话号码
		Long imsi = -10000L;
		if(!kv.get("IMSI").isEmpty())
		{
			String str = Util.findByRegex(kv.get("IMSI"), "[0-9]*", 0);
			if(str != null)
				imsi = Long.valueOf(kv.get("IMSI"));
		}
		String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
				
				
		//IMSI	imsi	bigint	imsi
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//IMEI	imei	character varying(32)	esn
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI	first_tmsi	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI	last_tmsi	integer	tmsi_d
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//MCC	mcc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MNC	mnc	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前LAC	sour_lac	smallint	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前CI	sour_ci	smallint	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后LAC	dest_lac	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后CI	dest_ci	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因	clearrequest_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因	clearcommand_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//位置更新完成时延	resp_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延	clear_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//城市ID	city_id	smallint	城市ID，根据采集的城市填写，济南是是：531
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);
				
		//小区系统号	cell_sys_id	bigint	【s_ip】查询mod_cell找ipint对应的cell_sys_id字段
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
				
		//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
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
		

		//运营商标识	op_id	smallint	移动1，联通2，电信3
		strLine.append("3" + subTemp.m_strNewFieldSplitSign);
		
		
		try {
			
				//监控
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
				log.error("监控记录异常",e);
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
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫类型call_type（case call_type when 0 then '主叫' when 1 then '被叫'
		//when 2 then '切换' when 3 then '紧急呼叫' when 4 then '业务重建' 
		//when 5 then '主叫切入' when 6 then '被叫切入'
		//else '其他' end as sub_type）
		int mo_mt = Integer.parseInt(kv.get("CALL_TYPE"));
		String sCalltype = "";
		switch(mo_mt)
		{
			case 0:
				sCalltype = "主叫";
				break;
			case 1:
				sCalltype = "被叫";
				break;
			case 2:
				sCalltype = "切换";
				break;
			case 3:
				sCalltype = "紧急呼叫";
				break;
			case 4:
				sCalltype = "业务重建";
				break;
			case 5:
				sCalltype = "主叫切入";
				break;
			case 6:
				sCalltype = "被叫切入";
				break;
			default:
				sCalltype = "其他";
				break;
		}
		strLine.append(sCalltype + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//感知掉话类型
		strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			}
			
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			}
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//对方IMSI
		//对方IMEI
		//对方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
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
		
		
		//连接号码
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//前转号码
		strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//其他号码
		strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//业务终止的LAC
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务终止的CI
		strLine.append(kv.get("END_CI") + subTemp.m_strNewFieldSplitSign);
		
		//源LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//源CI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//目标LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//目标CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//电路时隙号TS
		strLine.append(kv.get("CIC5") + subTemp.m_strNewFieldSplitSign);
		
		//电路识别码PCM
		strLine.append(kv.get("CIC7") + subTemp.m_strNewFieldSplitSign);
		
		//切换类型
		strLine.append(kv.get("HO_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//切换原因
		strLine.append(kv.get("HO_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//切换结果
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号1
		strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号2
		strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
		
		//切换拒绝原因
		strLine.append(kv.get("HOREJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//切换失败原因
		strLine.append(kv.get("HOFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//断连原因
		strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//断连发起方向
		strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//网络寻呼响应时延
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//建立时延
		strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//振铃时延
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//连接（应答）时延
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//断连时延
		strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//含短信个数
		strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫保持次数
		strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫恢复次数
		strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF启动次数
		strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF拒绝次数
		strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//通话时长
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//TCH占用时长
		strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
		
		//本方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//对方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//小区名称
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
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//短信类型
        strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
				
		//业务类型
		//sms_type （case sms_type when 0 then '短信发送' when 1 then '短信接收' 
		//when 2 then '短信提交报告' when 3 then '短信下发报告' when 4 then '短信状态报告' 
		//when 5 then 'WAP PUSH短信接收' when 6 then '被叫切入' else '其他' end as sub_type）
		int servicetype = Integer.parseInt(kv.get("SMS_TYPE"));
		String sServicetype = "";
		switch(servicetype)
		{
			case 0:
				sServicetype = "短信发送";
				break;
			case 1:
				sServicetype = "短信接收";
				break;
			case 2:
				sServicetype = "短信提交报告";
				break;
			case 3:
				sServicetype = "短信下发报告";
				break;
			case 4:
				sServicetype = "短信状态报告";
				break;
			case 5:
				sServicetype = "WAP PUSH短信接收";
				break;
			case 6:
				sServicetype = "被叫切入";
				break;
			default:
				sServicetype = "其他";
				break;
		}
		
		strLine.append(sServicetype + subTemp.m_strNewFieldSplitSign);
		
		
		
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//短信中心号码
		strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			}
			
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//对方IMSI
		//对方IMEI
		//对方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//短信业务模式
		strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//短信长度
		strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//网络寻呼响应时延
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//短信发送/接收总时延
		strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
		
		//本方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//对方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//小区名称
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		//业务类型
		//mm_type （case mm_type when 0 then 'LAC位置更新' when 1 then '周期位置更新' 
		//when 2 then '开机' when 3 then '关机' else '其他' end as sub_type）
		int nMMType = Integer.parseInt(kv.get("MM_TYPE"));
		String sMMType = "";
		
		switch(nMMType)
		{
			case 0:
				sMMType = "LAC位置更新";
				break;
			case 1:
				sMMType = "周期位置更新";
				break;
			case 2:
				sMMType = "开机";
				break;
			case 3:
				sMMType = "关机";
				break;
			default:
				sMMType = "其他";
				break;
		}
		
		strLine.append(sMMType + subTemp.m_strNewFieldSplitSign);
		
		
		
		//业务详细结果
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新拒绝原因
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
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前CI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新完成时延
		strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(this.collectObjInfo.getDevInfo().getCityID() + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
		
		//本方归属城市ID
		
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//小区名称
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC/RNC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		//service_type（case result when 0 then '呼叫寻呼' when 1 then '短信寻呼' 
		//when 2 then '位置更新寻呼' 
		//when 3 then '补充业务寻呼' else '其他' end as sub_type）
		int nServicetype = Integer.parseInt(kv.get("SERVICE_TYPE"));
		String sServicetype = "";
		
		switch(nServicetype)
		{
			case 0:
				sServicetype = "呼叫寻呼";
				break;
			case 1:
				sServicetype = "短信寻呼";
				break;
			case 2:
				sServicetype = "位置更新寻呼";
				break;
			case 3:
				sServicetype = "补充业务寻呼";
				break;
			default:
				sServicetype = "其他";
				break;
		}
		
		strLine.append(sServicetype + subTemp.m_strNewFieldSplitSign);
		
		//寻呼业务类型
		String result = kv.get("RESULT");
		if(result.equals("65535"))
			result = "255";
		strLine.append(result + subTemp.m_strNewFieldSplitSign);
		
		//被叫IMSI
		strLine.append(kv.get("CALLED_IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//被叫IMEI
		strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//被叫号码
		strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发生所在的rncID
		strLine.append(kv.get("RNC_ID") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//网络寻呼响应时延
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//网络信息
		strLine.append(kv.get("NET_INFO") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
		
		//本方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//小区名称
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫类型
		//call_type （case call_type when 0 then '主叫' when 1 then '被叫' 
		//when 2 then '切换' when 3 then '紧急呼叫' when 4 then '业务重建' 
		//when 5 then '主叫切入' when 6 then '被叫切入' else '其他' end as sub_type）

		int mo_mt = Integer.parseInt(kv.get("CALL_TYPE"));
		String sCalltype = "";
		switch(mo_mt)
		{
			case 0:
				sCalltype = "主叫";
				break;
			case 1:
				sCalltype = "被叫";
				break;
			case 2:
				sCalltype = "切换";
				break;
			case 3:
				sCalltype = "紧急呼叫";
				break;
			case 4:
				sCalltype = "业务重建";
				break;
			case 5:
				sCalltype = "主叫切入";
				break;
			case 6:
				sCalltype = "被叫切入";
				break;
			default:
				sCalltype = "其他";
				break;
		}
		strLine.append(sCalltype + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//感知掉话类型
		strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			}
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//对方IMSI
		//对方IMEI
		//对方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//连接号码
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//前转号码
		strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//其他号码
		strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI
		strLine.append(kv.get("START_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务终止的LAC
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务终止的CI
		strLine.append(kv.get("END_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//源LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//源CI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//目标LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//目标CI
		strLine.append(kv.get("DEST_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//电路时隙号TS	cic5	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//电路识别码PCM	cic7	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换类型
		strLine.append(kv.get("INFO_TRANS_CAP") + subTemp.m_strNewFieldSplitSign);
				
		//切换原因
		strLine.append(kv.get("RELOC_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//切换结果
		strLine.append(kv.get("RELOC_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号1
		strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号2
		strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
		
		//切换拒绝原因	horeject_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//切换失败原因
		strLine.append(kv.get("RELOCFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//断连原因
		strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//断连发起方向
		strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因	clearrequest_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		//清除命令原因	clearcommand_cause	smallint
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//网络寻呼响应时延
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//建立时延
		strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//振铃时延
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//连接（应答）时延
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//断连时延
		strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延	clear_delay	integer
		strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//含短信个数
		strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//呼叫保持次数
		strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//呼叫恢复次数
		strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//DTMF启动次数
		strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		//DTMF拒绝次数
		strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
				
		
		//通话时长
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//占用时长
		strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
		
		//本方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//对方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//小区名称
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//短信类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
				
		//业务类型
		//sms_type （case sms_type when 0 then '短信发送' when 1 then '短信接收' 
		//when 2 then '短信提交报告' when 3 then '短信下发报告' when 4 then '短信状态报告' 
		//when 5 then 'WAP PUSH短信接收' when 6 then '被叫切入' else '其他' end as sub_type）
		int servicetype = Integer.parseInt(kv.get("SMS_TYPE"));
		String sServicetype = "";
		switch(servicetype)
		{
			case 0:
				sServicetype = "短信发送";
				break;
			case 1:
				sServicetype = "短信接收";
				break;
			case 2:
				sServicetype = "短信提交报告";
				break;
			case 3:
				sServicetype = "短信下发报告";
				break;
			case 4:
				sServicetype = "短信状态报告";
				break;
			case 5:
				sServicetype = "WAP PUSH短信接收";
				break;
			case 6:
				sServicetype = "被叫切入";
				break;
			default:
				sServicetype = "其他";
				break;
		}
				
		strLine.append(sServicetype + subTemp.m_strNewFieldSplitSign);
		
		
		
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//短信中心号码
		strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty()|| msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLED");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn == "-10000")
			{
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			}
			
			strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//对方IMSI
		//对方IMEI
		//对方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		
		//业务发起的LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的SAC
		strLine.append(kv.get("START_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志
		strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//短信业务模式
		strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放请求原因
		strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放命令原因
		strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//短信长度
		strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		
		//网络寻呼响应时延
		strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放时延
		strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//短信发送/接收总时延
		strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
		
		//本方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//对方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//小区名称
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
		
		//开始时间
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		//业务类型
		//mm_type （case mm_type when 0 then 'LAC位置更新' when 1 then '周期位置更新' 
		//when 2 then '开机' when 3 then '关机' else '其他' end as sub_type）
		int nMMType = Integer.parseInt(kv.get("MM_TYPE"));
		String sMMType = "";
				
		switch(nMMType)
		{
			case 0:
				sMMType = "LAC位置更新";
				break;
			case 1:
				sMMType = "周期位置更新";
				break;
			case 2:
				sMMType = "开机";
				break;
			case 3:
				sMMType = "关机";
				break;
			default:
				sMMType = "其他";
				break;
		}
				
		strLine.append(sMMType + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新拒绝原因
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
				msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			}
		}
		
		
		
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//IMEI
		strLine.append(kv.get("IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前CI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后CI
		strLine.append(kv.get("DEST_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放请求原因
		strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放命令原因
		strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//网络响应时延
		strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放时延
		strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
		
		//本方归属城市ID
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
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		
		//小区名称
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
