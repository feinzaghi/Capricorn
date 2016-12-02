package com.turk.parser.taurus;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.log4j.Logger;

import com.turk.parser.taurus.model.MOD_MONITOR_MOBILE;

//import com.turk.task.SendSMS;
import com.turk.util.LogMgr;

public class MonitorHit {
	
	private static MonitorHit _instance = null;
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private ImportData _impMonitorHit  = new ImportData();
	
	public MonitorHit getInstance()
	{
		if(_instance == null)
			_instance = new MonitorHit("");
		return _instance;
	}
	
	public MonitorHit(String filename)
	{
		//filename = filename.substring(0,filename.indexOf("."));
		_impMonitorHit = new ImportData();
		//_impMonitorHit.SetFlieName(SystemConfig.getInstance().getCurrentPath() + File.separator 
		//		+  "mod_monitor_hit_" + filename +".txt","");
		
		try {
			//_impMonitorHit.Open();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Open MonitorHit", e);
		}
	}
	
	/**
	 * 
	 * @param mobilenum
	 * @param cdrTime
	 * @param cellname
	 * @param calltype 
	 * @param datatype 数据类型 1:cmcc 2:unicom 3:telecom + 1：cc 2:sm 3:mm 
	 * @return
	 */
	public boolean IsTouchNet(String mobilenum,String omsisdn,Date cdrTime,String cellname,long cellsysid,
			int calltype,int callduration,int datatype)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getTouchMobile(mobilenum);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("TouchNet用户:" +  mobilenum + " 不在监控时间范围内!");
				return false; //不在监控时间范围内
			}
			
			
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "";
			switch(datatype)
			{
				case 11:
				case 21:
				case 31:
				{
					//TELECOM CC
					//0：主叫；1：被叫；2：切入；3：紧急。  + 呼叫时长call_duration（换行为秒）
					switch(calltype)
					{
						case 0:
							action = "主叫" + callduration/10000 + "秒";
							break;
						case 1:
							action = "被叫" + callduration/10000 + "秒";
							break;
						case 2:
							action = "切入" + callduration/10000 + "秒";
							break;
						case 3:
							action = "紧急" + callduration/10000 + "秒";
							break;
						case 4:
							action = "业务重建" + callduration/10000 + "秒";
							break;
						case 5:
							action = "主叫切入" + callduration/10000 + "秒";
							break;
						case 6:
							action = "被叫切入" + callduration/10000 + "秒";
							break;
						default:
							action = "其他" + callduration/10000 + "秒";
							break;
					}
				}
					break;
				case 12:
				case 22:
				case 32:
				{//SM：0 短信发送；1 短信接收。
					//case sms_type when 0 then '短信发送' when 1 then '短信接收' 
					//when 2 then '短信提交报告' when 3 then '短信下发报告' 
					//when 4 then '短信状态报告' when 5 then 'WAP PUSH短信接收' 
					//when 6 then '被叫切入' else '其他' end as sub_type
					switch(calltype)
					{
						case 0:
							action = "短信发送";
							break;
						case 1:
							action = "短信接收";
							break;
						case 3:
							action = "短信下发报告";
							break;
						case 4:
							action = "短信状态报告";
							break;
						case 5:
							action = "WAP PUSH短信接收";
							break;
						case 6:
							action = "被叫切入";
							break; 
						default:
							action = "其他";
							break;
					}
				}
					break;
				case 13:
				case 23:
				{
					//case mm_type when 0 then 'LAC位置更新' 
					//when 1 then '周期位置更新' when 2 then '开机' 
					//when 3 then '关机' else '其他' end as sub_type
					switch(calltype)
					{
						case 0:
							action = "LAC位置更新";
							break;
						case 1:
							action = "周期位置更新";
							break;
						case 2:
							action = "开机";
							break;
						case 3:
							action = "关机";
							break;
						default:
							action = "其他";
							break;
					}
				}
					break;
				case 33:
				{//MM
					switch(calltype)
					{
						case 0:
							action = "基于时间";
							break;
						case 1:
							action = "开机";
							break;
						case 2:
							action = "基于区域";
							break;
						case 3:
							action = "关机";
							break;
						case 4:
							action = "参数更改";
							break;
						case 5:
							action = "基于指令";
							break;
						case 6:
							action = "基于距离";
							break;
						case 7:
							action = "基于用户区";
							break;
						case 9:
							action = "BCMC登记";
							break;
					}
				}
					break;
				
			}
			String alarm_level = mobile.getAlarmLevel();
			String msisdn = mobilenum;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name, cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null && !sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控 %s在%s,%s%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action,omsisdn);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	
	
	public void CommitTouchHit()
	{
		try {
			//_impMonitorHit.Commit("");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("上传TouchHit文件异常",e);
		}
		
		//MonitorMobileConfig.getInstance().Clear();
	}
	
	
	public boolean IsPowerUp(String mobilenum,Date cdrTime,String cellname,long cellsysid,
			int regtype)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getPowerMobile(mobilenum);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				String strdebug = String.format("PowerUp用户:%s不在监控时间范围内,START[%s] END[%s] CURRENT[%s]", 
						mobilenum,mobile.getStartTime(),mobile.getEndTime(),cdrTime);
				log.debug(strdebug);
				return false; //不在监控时间范围内
			}
			
			
			if (regtype != 1 && regtype != 3)
			{
				String strdebug = String.format("PowerUp用户:%s状态%d,START[%s] END[%s] CURRENT[%s]", 
						mobilenum,regtype,mobile.getStartTime(),mobile.getEndTime(),cdrTime);
				log.debug(strdebug);
				return false; //不符合监控条件
			}
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "";
			switch(regtype)
			{
				case 1:
					action = "开机";
					break;
				case 3:
					action = "关机";
					break;
			}
			
			String alarm_level = mobile.getAlarmLevel();
			String msisdn = mobilenum;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(
					mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name,cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null && !sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控%s在%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	
	
	public boolean IsCallNum(String mobilenum,Date cdrTime,String cellname,long cellsysid,
			String o_mobilenum)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getCallNumMobile(mobilenum);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				String strdebug = String.format("CallNum用户:%s不在监控时间范围内,START[%s] END[%s] CURRENT[%s]", 
						mobilenum,mobile.getStartTime(),mobile.getEndTime(),cdrTime);
				log.debug(strdebug);
				return false; //不在监控时间范围内
			}
			
			if(o_mobilenum.isEmpty() || !mobile.getCallNumList().contains(o_mobilenum))
			{//不是特定人员监控信息
				String strdebug = String.format("CallNum用户:%s %s不是特定人员监控信息", 
						mobilenum,o_mobilenum);
				log.debug(strdebug);
				return false; //不在监控时间范围内
			}
			
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "特定监控用户 -:" + " 监控号码:[" + mobilenum + "] 联系号码:[" + o_mobilenum + "]";
			
			String alarm_level = mobile.getAlarmLevel();
			String msisdn = o_mobilenum;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(
					mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name,cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null && !sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控%s在%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	
	
	public boolean IsExitArea(String mobilenum,Date cdrTime,String cellname,long cellsysid)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getExitAreaMobile(mobilenum);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("ExitArea用户:" +  mobilenum + " 不在监控时间范围内!");
				return false; //不在监控时间范围内
			}
			
			if(mobile.getExitAreaCellList().contains(cellname) || cellname.isEmpty())
			{//正常
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "用户已离开监控区域，目前位置["+ cellname +"]";
			
			
			
			
			String alarm_level = mobile.getAlarmLevel();
			String msisdn = mobilenum;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(
					mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name, cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null && !sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控%s在%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	
	public boolean IsEnterArea(String mobilenum,Date cdrTime,String cellname,long cellsysid)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getEnterAreaMobile(mobilenum);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("EnterArea用户:" +  mobilenum + " 不在监控时间范围内!");
				return false; //不在监控时间范围内
			}
			
			if(!mobile.getEnterAreaCellList().contains(cellname) || cellname.isEmpty())
			{//正常
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "用户已进入监控区域，目前位置["+ cellname +"]";
			
					
			String alarm_level = mobile.getAlarmLevel();
			String msisdn = mobilenum;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(
					mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name, cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null && !sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控%s在%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	
	public boolean IsExitLAC(String mobilenum,Date cdrTime,String cellname,int lac,long cellsysid)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getExitLACMobile(mobilenum);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("ExitLAC用户:" +  mobilenum + " 不在监控时间范围内!");
				return false; //不在监控时间范围内
			}
		
			
			if(mobile.getExitLACList().containsKey(lac))
			{//正常
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "用户已离开监控区域，目前LAC["+ lac +"]";
			
			String alarm_level = mobile.getAlarmLevel();
			String msisdn = mobilenum;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(
					mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name, cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null && !sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控%s在%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	
	
	public boolean IsEnterLAC(String mobilenum,Date cdrTime,String cellname,int lac,long cellsysid)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getEnterLACMobile(mobilenum);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("EnterLAC用户:" +  mobilenum + " 不在监控时间范围内!");
				return false; //不在监控时间范围内
			}
			
			if(!mobile.getEnterLACList().containsKey(lac))
			{//正常
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "用户已进入监控区域，目前LAC["+ lac +"]";
			
			String alarm_level = mobile.getAlarmLevel();
			String msisdn = mobilenum;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(
					mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name, cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null && !sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控%s在%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	
	public boolean BlongLacArea(String cityid,String msisdn,Date cdrTime,
			String cellname,int lac,long cellsysid)
	{
		ArrayList<MOD_MONITOR_MOBILE> list = MonitorMobileConfig.getInstance().getBlongLACArea(cityid);
		
		if(list == null)
			return false;
		
		for(MOD_MONITOR_MOBILE mobile : list)
		{
			if(mobile == null)
				return false;//不是监控号码
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("归属地区域监控-不在监控时间范围内!");
				return false; //不在监控时间范围内
			}
			
			if(!mobile.getEnterLACList().containsKey(lac)&&
					(!mobile.getEnterAreaCellList().contains(cellname) || cellname.isEmpty()))
			{//正常
				return false;
			}
			
					
			//用户号码排除监控 白名单
			if(mobile.getExcludeNumList()!=null && mobile.getExcludeNumList().containsKey(msisdn))
				return false;
			
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "归属地[" + cityid + "]";
			
			String alarm_level = mobile.getAlarmLevel();
			//String msisdn = msisdn;
			String confirm = "";
			Date confirm_time = new Date();
			String  confirm_name = "";
			
			_impMonitorHit.ImportMonitorHit(
					mobile.getMonitorID(),mobile.getUserName(),
					monitor_name, case_id, hit_item, hit_time, cell_name, cellsysid, action, 
					alarm_level, msisdn, confirm, confirm_time, confirm_name);
			
			String sendlist = mobile.getSendMsgList();
			if(sendlist!=null&&!sendlist.trim().isEmpty())
			{
				//发送内容：
				//hit_time + case_id + "案件" + monitor_name + "布控" + misidn + "号码在" + cell_name + "中标，内容：" + action 
				//如下图第一条:2013-5-16 09:58:09 JN3421案件触网短信测试布控15315115936号码在53122_21039_银座花园中标，内容：短信接收
				SimpleDateFormat formatter = new SimpleDateFormat("dd日 HH:mm:ss");
				String content = String.format("%s%s布控%s在%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	

}
