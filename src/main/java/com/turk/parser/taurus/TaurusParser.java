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
		
		//动作类型	mo_mt	"【call_type】，
		strLine.append(kv.get("CALL_TYPE") + subTemp.m_strNewFieldSplitSign);
		
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
		
		//对方号码	mt_msisdn	character varying(32)	"当动作类型为0：【被叫号码】 当动作类型为1：【主叫号码】
		int mo_mt = 0;
		String sMo_mt = Util.findByRegex(kv.get("CALL_TYPE"), "[0-9]*", 0);
		if(sMo_mt != null)
			mo_mt = Integer.parseInt(sMo_mt);
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0：【被叫号码】
			//正则表达式，判断是否为数字
			//String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
		}
		else if(mo_mt == 1)
		{//1：【主叫号码】
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}

		//本方IMSI	imsi	bigint	【IMSI】
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//本方设备号	esn	character varying(32)	【ESN】
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//时长	call_duration	integer	【清除时延】，取整
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
			//本方归属省份	province	character varying(32)	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的省份
			strLine.append(home1.getProvince() + subTemp.m_strNewFieldSplitSign);
			
			//本方归属城市	city	character varying(32)	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市
			strLine.append(home1.getCity() + subTemp.m_strNewFieldSplitSign);
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//本方归属运营商	operator	character varying(32)	暂时为空
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
			//对方归属国家	mt_country	character varying(32)	暂时为空
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			
			//对方归属省份	mt_province	character varying(32)	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的省份
			strLine.append(home2.getProvince() + subTemp.m_strNewFieldSplitSign);
			//对方归属城市	mt_city	character varying(32)	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市
			strLine.append(home2.getCity() + subTemp.m_strNewFieldSplitSign);
			
			//对方归属城市ID	o_city_id	smallint	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//对方归属运营商	mt_operator	character varying(32)	暂时为空
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
		
		//阿朗CI	ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空
		MOD_CELL cell = null;
		if(sOPC.equals("-1"))
		{//阿朗
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
		{//中兴
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
		
		//源IP	s_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//目的IP	d_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI	tmsi_o	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI	tmsi_d	integer	tmsi_d
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//源LAC	sour_lac	integer	sour_lac
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//源CI	sour_ci	integer	sour_ci
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC	start_lac	integer	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI	start_ci	integer	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//业务终止的LAC	end_lac	integer	end_lac
		strLine.append(kv.get("END_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务终止的CI	end_ci	integer	end_ci
		strLine.append(kv.get("END_CI") + subTemp.m_strNewFieldSplitSign);
		
		//目标LAC	dest_lac	integer	dest_lac
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//目标CI	dest_ci	integer	dest_ci
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果	a_result	smallint	a_result
		strLine.append(kv.get("A_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//BSC内部切换次数	ho_num	smallint	ho_num
		strLine.append(kv.get("HO_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//清除原因	clear_cause	smallint	clear_cause
		strLine.append(kv.get("CLEAR_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//用户关机	a_close_reg	smallint	a_close_reg
		strLine.append(kv.get("A_CLOSE_REG") + subTemp.m_strNewFieldSplitSign);
		
		//应答时延	conn_time	smallint	conn_time
		strLine.append(kv.get("CONN_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//闪动信息个数	fl_num	smallint	fl_num
		strLine.append(kv.get("FL_NUM") + subTemp.m_strNewFieldSplitSign);

		
		try {
			//监控
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
			
			
			//实时用户监听
			String data = "CC_TELECOM:" + strLine 
					+ lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			SocketMonitor(strimsi,msisdn,data);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("监控记录异常",e);
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
		
		//mo_mt	smallint	"【sms_type】，0：短信发送MO；1：短信接受MT。"
		strLine.append(kv.get("SMS_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		int mo_mt = 0;
		
		if(kv.get("SMS_TYPE") != null)
			mo_mt = Integer.parseInt(kv.get("SMS_TYPE"));
		
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
		
		//o_msisdn	character varying(32)	"当动作类型为0：【被叫号码】
		String mt_msisdn = "";
		if(mo_mt == 0)
		{//0：【被叫号码】
			//正则表达式，判断是否为数字
			//String str = Util.findByRegex(kv.get("CALLED"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLED");
			strLine.append(kv.get("CALLED") + subTemp.m_strNewFieldSplitSign);
			
		}
		else if(mo_mt == 1)
		{//1：【主叫号码】
			//String str = Util.findByRegex(kv.get("CALLING"), "[0-9]*", 0);
			mt_msisdn = kv.get("CALLING");
			strLine.append(kv.get("CALLING") + subTemp.m_strNewFieldSplitSign);
		}
		
		//imsi	bigint	【imsi】
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//esn	character varying(32)	【esn】
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		
		//result	smallint	【mm_result】，0表示成功，1表示失败
		strLine.append(kv.get("SMS_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//province	character varying(32)	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的省份
		//city	character varying(32)	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市
		//operator	character varying(32)	暂时为空
		int sectionno = 0;
		if(msisdn.length() >= 7)
		{
			sectionno = Integer.parseInt(String.valueOf(msisdn).substring(0,7));
		}
		
		String belongCity = "0";
		
		CFG_NUM_HOME home1 = NumHome.getInstance().getRegion(sectionno);
		if(home1 != null)
		{
			//本方归属省份	province	character varying(32)	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的省份
			strLine.append(home1.getProvince() + subTemp.m_strNewFieldSplitSign);
			
			//本方归属城市	city	character varying(32)	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市
			strLine.append(home1.getCity() + subTemp.m_strNewFieldSplitSign);
			
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//本方归属运营商	operator	character varying(32)	暂时为空
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
		
		//o_country	character varying(32)	暂时为空
		//o_province	character varying(32)	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的省份
		//o_city	character varying(32)	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市
		//o_operator	character varying(32)	暂时为空
		
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
			//对方归属国家	mt_country	character varying(32)	暂时为空
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
			
			//对方归属省份	mt_province	character varying(32)	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的省份
			strLine.append(home2.getProvince() + subTemp.m_strNewFieldSplitSign);
			//对方归属城市	mt_city	character varying(32)	通过关联出的对方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市
			strLine.append(home2.getCity() + subTemp.m_strNewFieldSplitSign);
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home2.getCityID() + subTemp.m_strNewFieldSplitSign);
			
			//对方归属运营商	mt_operator	character varying(32)	暂时为空
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
		
		//阿朗CI	ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空
		String sOPC = kv.get("OPC");
		int nCityID = 531;
		
		//阿朗CI	ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空
		MOD_CELL cell = null;
		int sIP = Integer.parseInt(kv.get("S_IP"));
		if(sOPC.equals("-1"))
		{//阿朗
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
		{//中兴
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
		
		//源IP	s_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//目的IP	d_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC	start_lac	integer	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI	start_ci	integer	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI	tmsi_o	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
	
		try {
				if(_monitor!=null)
				{
					//监控
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
				
				//实时用户监听
				
				String data = "SM_TELECOM:" + strLine
						+ lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(strimsi,msisdn,data);
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("监控记录异常",e);
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
		strLine.append(kv.get("MM_TYPE") + subTemp.m_strNewFieldSplitSign);
		
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
		String belongCity = "0";
		if(home1 != null)
		{
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//imsi	bigint	"【mdn】4600开头的"	【mdn】
		strLine.append(kv.get("IMSI") + subTemp.m_strNewFieldSplitSign);
		
		//esn	character varying(32)	【esn】	【esn】
		strLine.append(kv.get("ESN") + subTemp.m_strNewFieldSplitSign);
		//ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空

		//阿朗CI	ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空
				
		String sOPC = kv.get("OPC");
		int nCityID = 531;
		
		//阿朗CI	ci	character varying(32)	【s_ip】查询al_cellid表对应的基站名称，不能匹配置空
		MOD_CELL cell = null;
		int sIP = Integer.parseInt(kv.get("S_IP"));
		if(sOPC.equals("-1"))
		{//阿朗
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
		{//中兴
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
		
		//源IP	s_ip	integer	s_ip
		strLine.append(kv.get("S_IP") + subTemp.m_strNewFieldSplitSign);
		
		//目的IP	d_ip	integer	d_ip
		strLine.append(kv.get("D_IP") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI	tmsi_o	integer	tmsi_o
		strLine.append(kv.get("TMSI_O") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI	tmsi_d	integer	tmsi_d
		strLine.append(kv.get("TMSI_D") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC	start_lac	integer	start_lac
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI	start_ci	integer	start_ci
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//mm_result	smallint	mm_result
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		
		try {
				if(_monitor!=null)
				{	//监控
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
				
				//实时用户监听
				
				String data = "MM_TELECOM:" + strLine
						+ lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("监控记录异常",e);
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
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫类型
		strLine.append(kv.get("CALL_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//感知掉话类型
		//strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
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
		
		
		//连接号码
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//前转号码
		//strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//其他号码
		//strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
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
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//电路时隙号TS
		//strLine.append(kv.get("CIC5") + subTemp.m_strNewFieldSplitSign);
		
		//电路识别码PCM
		//strLine.append(kv.get("CIC7") + subTemp.m_strNewFieldSplitSign);
		
		//切换类型
		strLine.append(kv.get("HO_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//切换原因
		//strLine.append(kv.get("HO_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//切换结果
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号1
		//strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
		
		//切换参考号2
		//strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
		
		//切换拒绝原因
		//strLine.append(kv.get("HOREJECT_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//切换失败原因
		//strLine.append(kv.get("HOFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//断连原因
		//strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//断连发起方向
		//strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		//strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		//strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//网络寻呼响应时延
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//建立时延
		//strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//振铃时延
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//连接（应答）时延
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//断连时延
		//strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		//strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//含短信个数
		//strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫保持次数
		//strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫恢复次数
		//strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF启动次数
		//strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//DTMF拒绝次数
		//strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
		
		//通话时长
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//TCH占用时长
		//strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
		String belongCity = "0";
		if(home1 != null)
		{
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//对方归属城市ID
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
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
				if(_monitor!=null)
				{
					//监控
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
				
				//实时用户监听
				
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("监控记录异常",e);
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
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//短信类型
		strLine.append(kv.get("SMS_TYPE") + subTemp.m_strNewFieldSplitSign);
		int smstype = Integer.parseInt(kv.get("SMS_TYPE"));
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//短信中心号码
		//strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn.equals("-10000"))
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
			
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
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
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
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
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
			
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的CI
		strLine.append(kv.get("START_CI") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//短信业务模式
		//strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		//strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		//strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//短信长度
		//strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		//strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//网络寻呼响应时延
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		//strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//短信发送/接收总时延
		//strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
		String belongCity = "0";
		if(home1 != null)
		{
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//对方归属城市ID
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
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
				if(_monitor!=null)
				{
					//监控
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
				
				//实时用户监听
				
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("监控记录异常",e);
			}
		
		if(subTemp.m_hasRowkey)
		{//根据Hbase的要求，增加rowkey字段
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新类型
		strLine.append(kv.get("MM_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新拒绝原因
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
			msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
		}
		
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI
		strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
		//IMEI
		strLine.append(kv.get("IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前CI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		//strLine.append(kv.get("CLEARREQUEST_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		//strLine.append(kv.get("CLEARCOMMAND_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新完成时延
		//strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		//strLine.append(kv.get("CLEAR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
		String belongCity = "0";
		if(home1 != null)
		{
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
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
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
			if(_monitor!=null)
			{
				//监控
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
			
			//实时用户监听
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("监控记录异常",e);
		}

		if(subTemp.m_hasRowkey)
		{//根据Hbase的要求，增加rowkey字段
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
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
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
		String cellkey = oper + "_" +  kv.get("START_LAC") + "_" + kv.get("START_CI");
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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

		
		return strLine.toString();
	}
	
	public String parsercdr51(LineTempletP.SubTemplet subTemp,
			Map<String,String> kv,int cityID,int oper)
	{
		StringBuffer strLine = new StringBuffer();
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);
		
		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//呼叫类型
		strLine.append(kv.get("CALL_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//感知掉话类型
		//strLine.append(kv.get("DROP_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
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
			
			if(msisdn.isEmpty() || msisdn.equals("-10000"))
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
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
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
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		
		//连接号码
		strLine.append(kv.get("CONNECTED_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//前转号码
		//strLine.append(kv.get("REDIRECT_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//其他号码
		//strLine.append(kv.get("OTHER_NUM") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
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
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		
		//电路时隙号TS	cic5	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//电路识别码PCM	cic7	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//切换类型
		strLine.append(kv.get("INFO_TRANS_CAP") + subTemp.m_strNewFieldSplitSign);
						
		//切换原因
		//strLine.append(kv.get("RELOC_CAUSE") + subTemp.m_strNewFieldSplitSign);
				
		//切换结果
		strLine.append(kv.get("RELOC_RESULT") + subTemp.m_strNewFieldSplitSign);
				
		//切换参考号1
		//strLine.append(kv.get("HO_REF1") + subTemp.m_strNewFieldSplitSign);
				
		//切换参考号2
		//strLine.append(kv.get("HO_REF2") + subTemp.m_strNewFieldSplitSign);
				
		//切换拒绝原因	horeject_cause	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//切换失败原因
		//strLine.append(kv.get("RELOCFAILURE_CAUSE") + subTemp.m_strNewFieldSplitSign);
				
		//断连原因
		//strLine.append(kv.get("DISCON_CAUSE") + subTemp.m_strNewFieldSplitSign);
				
		//断连发起方向
		//strLine.append(kv.get("DISCON_DIRECT") + subTemp.m_strNewFieldSplitSign);
				
		//清除请求原因	clearrequest_cause	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		//清除命令原因	clearcommand_cause	smallint
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
				
		//用户寻呼响应时延
		strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//网络寻呼响应时延
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//建立时延
		//strLine.append(kv.get("SETUP_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//振铃时延
		strLine.append(kv.get("ALERT_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//连接（应答）时延
		strLine.append(kv.get("CONN_DELAY") + subTemp.m_strNewFieldSplitSign);
						
		//断连时延
		//strLine.append(kv.get("DISCON_DELAY") + subTemp.m_strNewFieldSplitSign);
				
		//BSSMAP清除时延	clear_delay	integer
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);

		//含短信个数
		//strLine.append(kv.get("SM_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//呼叫保持次数
		//strLine.append(kv.get("HOLD_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//呼叫恢复次数
		//strLine.append(kv.get("RETRIEVE_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//DTMF启动次数
		//strLine.append(kv.get("DTMF_COUNT") + subTemp.m_strNewFieldSplitSign);
						
		//DTMF拒绝次数
		//strLine.append(kv.get("DTMFREFUSE_COUNT") + subTemp.m_strNewFieldSplitSign);
						
				
		//通话时长
		strLine.append(kv.get("TALK_TIME") + subTemp.m_strNewFieldSplitSign);
				
		//占用时长
		//strLine.append(kv.get("SEIZE_TIME") + subTemp.m_strNewFieldSplitSign);
				
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
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
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
		String belongCity = "0";
		if(home1 != null)
		{
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
		//对方归属城市ID
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
					//监控
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
				
				//实时用户监听
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("监控记录异常",e);
			}
		
		if(subTemp.m_hasRowkey)
		{//根据Hbase的要求，增加rowkey字段
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//短信类型
		strLine.append(kv.get("SMS_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		String sSmsType = Util.findByRegex(kv.get("SMS_TYPE"), "[0-9]*", 0);
		int smstype = -1;
		if(sSmsType!=null)
			smstype = Integer.parseInt(kv.get("SMS_TYPE"));
		
		//业务详细结果
		strLine.append(kv.get("RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//短信中心号码
		//strLine.append(kv.get("SMSC") + subTemp.m_strNewFieldSplitSign);
		
		//本方IMSI
		//本方IMEI
		//本方号码
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
			
			//String msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
			strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
			strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			msisdn = kv.get("CALLING");
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			if(msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			if(msisdn.isEmpty()|| msisdn.equals("-10000"))
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
			if(msisdn!=null && msisdn.length() >= 11)
				msisdn = msisdn.substring(msisdn.length() - 11);
			
			if(msisdn.isEmpty() || msisdn.equals("-10000"))
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
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLED_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLED");
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
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
			//strLine.append(o_imsi + subTemp.m_strNewFieldSplitSign);
			//strLine.append(kv.get("CALLING_IMEI") + subTemp.m_strNewFieldSplitSign);
			o_msisdn = kv.get("CALLING");
			if(o_msisdn.length() > 11 && o_msisdn.startsWith("861"))
				o_msisdn = o_msisdn.substring(o_msisdn.length() - 11);
			//若为msisdn号码的长度为13位并且以86开头，则把86去除；
			strLine.append(o_msisdn + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			//strLine.append("" + subTemp.m_strNewFieldSplitSign);
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		
		//业务发起的LAC
		strLine.append(kv.get("START_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//业务发起的SAC
		strLine.append(kv.get("START_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//寻呼标志
		//strLine.append(kv.get("PR_FLAG") + subTemp.m_strNewFieldSplitSign);
		
		//短信业务模式
		//strLine.append(kv.get("SMS_MODE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放请求原因
		//strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放命令原因
		//strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//短信长度
		//strLine.append(kv.get("SMS_LENTH") + subTemp.m_strNewFieldSplitSign);
		
		//用户寻呼响应时延
		//strLine.append(kv.get("USRPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		
		//网络寻呼响应时延
		//strLine.append(kv.get("NETPR_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放时延
		//strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//短信发送/接收总时延
		//strLine.append(kv.get("SMS_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
				
		//小区系统号
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
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
		String belongCity = "0";
		if(home1 != null)
		{
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
		}
		else
		{
			strLine.append("" + subTemp.m_strNewFieldSplitSign);
		}
				
		//对方归属城市ID
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
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}


		try {
				if(_monitor!=null)
				{
					//监控
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
				
				//实时用户监听
				
				String data = subTemp.m_tag + ":"  + strLine + lon
						+ subTemp.m_strNewFieldSplitSign + lat;
				
				SocketMonitor(imsi.toString(),msisdn,data);
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				applog.error("监控记录异常",e);
			}	
		
		if(subTemp.m_hasRowkey)
		{//根据Hbase的要求，增加rowkey字段
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
		
		//开始时间
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新类型
		strLine.append(kv.get("MM_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("MM_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新拒绝原因
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
			msisdn = String.valueOf(MapImsiMsisdn.getInstance().getMSISDN(imsi));//本方号码
		}
		
		
		
		strLine.append(msisdn + subTemp.m_strNewFieldSplitSign);
		
		//IMSI
		strLine.append(imsi + subTemp.m_strNewFieldSplitSign);
		
		//IMEI
		strLine.append(kv.get("IMEI") + subTemp.m_strNewFieldSplitSign);
		
		//初始TMSI
		strLine.append(kv.get("FIRST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//终止TMSI
		strLine.append(kv.get("LAST_TMSI") + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("MNC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前CI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后CI
		strLine.append(kv.get("DEST_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放请求原因
		//strLine.append(kv.get("IU_RELREQ_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放命令原因
		//strLine.append(kv.get("IU_RELCOM_CAUSE") + subTemp.m_strNewFieldSplitSign);
		
		//网络响应时延
		//strLine.append(kv.get("RESP_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//Iu释放时延
		//strLine.append(kv.get("IU_REL_DELAY") + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
				
		//小区系统号
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
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
		String belongCity = "0";
		if(home1 != null)
		{
			//本方归属城市ID	s_city_id	smallint	通过关联出的本方电话号码，再通过号码归属地表（cfg_num_home）关联出其归属的地市ID
			strLine.append(home1.getCityID() + subTemp.m_strNewFieldSplitSign);
			belongCity = String.valueOf(home1.getCityID());
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
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		try {
			if(_monitor!=null)
			{
				//监控
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
			
			//实时用户监听
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("监控记录异常",e);
		}
		

		if(subTemp.m_hasRowkey)
		{//根据Hbase的要求，增加rowkey字段
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("BSC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新类型
		int hotype = Integer.parseInt(kv.get("HO_TYPE"));
		hotype = hotype + 200;
		strLine.append(hotype + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新拒绝原因
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
		//strLine.append(kv.get("SOUR_MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("SOUR_MNC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前CI
		strLine.append(kv.get("SOUR_CI") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//位置更新完成时延
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
		String cellkey = oper + "_" +  kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		String sLacCI = kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		
		try {
			/*
			if(_monitor!=null)
			{
				//监控
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
			
			//实时用户监听
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("监控记录异常",e);
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
		
		//开始时间
		strLine.append(kv.get("START_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//结束时间
		//strLine.append(kv.get("END_TIME") + subTemp.m_strNewFieldSplitSign);
		
		//CDR标识号
		//strLine.append(kv.get("CDR_ID") + subTemp.m_strNewFieldSplitSign);
		
		//BSC
		strLine.append(kv.get("RNC") + subTemp.m_strNewFieldSplitSign);

		//MSC
		strLine.append(kv.get("MSC") + subTemp.m_strNewFieldSplitSign);
		
		//MGW_IP
		//strLine.append(kv.get("MGW_IP") + subTemp.m_strNewFieldSplitSign);
		
		//MSC_IP
		//strLine.append(kv.get("MSC_IP") + subTemp.m_strNewFieldSplitSign);
		
		//业务类型
		strLine.append(kv.get("SERVICE_TYPE") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新类型
		int hotype = Integer.parseInt(kv.get("HO_TYPE"));
		hotype = hotype + 200;
		strLine.append(hotype + subTemp.m_strNewFieldSplitSign);
		
		//业务详细结果
		strLine.append(kv.get("HO_RESULT") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新拒绝原因
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
		strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//MCC
		//strLine.append(kv.get("SOUR_MCC") + subTemp.m_strNewFieldSplitSign);
		
		//MNC
		//strLine.append(kv.get("SOUR_MNC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前LAC
		strLine.append(kv.get("SOUR_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新前CI
		strLine.append(kv.get("SOUR_SAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后LAC
		strLine.append(kv.get("DEST_LAC") + subTemp.m_strNewFieldSplitSign);
		
		//位置更新后CI
		strLine.append(kv.get("DEST_CI") + subTemp.m_strNewFieldSplitSign);
		
		//清除请求原因
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//清除命令原因
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//位置更新完成时延
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//BSSMAP清除时延
		//strLine.append("" + subTemp.m_strNewFieldSplitSign);
		
		//城市ID
		strLine.append(cityID + subTemp.m_strNewFieldSplitSign);
		
		//小区系统号
		String cellkey = oper + "_" +  kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		String sLacCI = kv.get("DEST_LAC") + "_" + kv.get("DEST_CI");
		
		MOD_CELL cell = MapModCell.getInstance().getCellInfo(cellkey);
		if(cell != null)
		{
			strLine.append(cell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			//运营商编号*10000000000000+城市区号*10000000000+LAC*100000+CI
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
			strLine.append(sLacCI + subTemp.m_strNewFieldSplitSign);
		}
		
		
		try {
			/*
			if(_monitor!=null)
			{
				//监控
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
			
			//实时用户监听
			
			String data = subTemp.m_tag + ":"  + strLine + lon
					+ subTemp.m_strNewFieldSplitSign + lat;
			
			SocketMonitor(imsi.toString(),msisdn,data);
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			applog.error("监控记录异常",e);
		}
		return strLine.toString();
	}
	
	
	/**
	 * 发送监控socket消息
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
