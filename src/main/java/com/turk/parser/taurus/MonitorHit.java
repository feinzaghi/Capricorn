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
	 * @param datatype �������� 1:cmcc 2:unicom 3:telecom + 1��cc 2:sm 3:mm 
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("TouchNet�û�:" +  mobilenum + " ���ڼ��ʱ�䷶Χ��!");
				return false; //���ڼ��ʱ�䷶Χ��
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
					//0�����У�1�����У�2�����룻3��������  + ����ʱ��call_duration������Ϊ�룩
					switch(calltype)
					{
						case 0:
							action = "����" + callduration/10000 + "��";
							break;
						case 1:
							action = "����" + callduration/10000 + "��";
							break;
						case 2:
							action = "����" + callduration/10000 + "��";
							break;
						case 3:
							action = "����" + callduration/10000 + "��";
							break;
						case 4:
							action = "ҵ���ؽ�" + callduration/10000 + "��";
							break;
						case 5:
							action = "��������" + callduration/10000 + "��";
							break;
						case 6:
							action = "��������" + callduration/10000 + "��";
							break;
						default:
							action = "����" + callduration/10000 + "��";
							break;
					}
				}
					break;
				case 12:
				case 22:
				case 32:
				{//SM��0 ���ŷ��ͣ�1 ���Ž��ա�
					//case sms_type when 0 then '���ŷ���' when 1 then '���Ž���' 
					//when 2 then '�����ύ����' when 3 then '�����·�����' 
					//when 4 then '����״̬����' when 5 then 'WAP PUSH���Ž���' 
					//when 6 then '��������' else '����' end as sub_type
					switch(calltype)
					{
						case 0:
							action = "���ŷ���";
							break;
						case 1:
							action = "���Ž���";
							break;
						case 3:
							action = "�����·�����";
							break;
						case 4:
							action = "����״̬����";
							break;
						case 5:
							action = "WAP PUSH���Ž���";
							break;
						case 6:
							action = "��������";
							break; 
						default:
							action = "����";
							break;
					}
				}
					break;
				case 13:
				case 23:
				{
					//case mm_type when 0 then 'LACλ�ø���' 
					//when 1 then '����λ�ø���' when 2 then '����' 
					//when 3 then '�ػ�' else '����' end as sub_type
					switch(calltype)
					{
						case 0:
							action = "LACλ�ø���";
							break;
						case 1:
							action = "����λ�ø���";
							break;
						case 2:
							action = "����";
							break;
						case 3:
							action = "�ػ�";
							break;
						default:
							action = "����";
							break;
					}
				}
					break;
				case 33:
				{//MM
					switch(calltype)
					{
						case 0:
							action = "����ʱ��";
							break;
						case 1:
							action = "����";
							break;
						case 2:
							action = "��������";
							break;
						case 3:
							action = "�ػ�";
							break;
						case 4:
							action = "��������";
							break;
						case 5:
							action = "����ָ��";
							break;
						case 6:
							action = "���ھ���";
							break;
						case 7:
							action = "�����û���";
							break;
						case 9:
							action = "BCMC�Ǽ�";
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s���� %s��%s,%s%s",
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
			log.error("�ϴ�TouchHit�ļ��쳣",e);
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				String strdebug = String.format("PowerUp�û�:%s���ڼ��ʱ�䷶Χ��,START[%s] END[%s] CURRENT[%s]", 
						mobilenum,mobile.getStartTime(),mobile.getEndTime(),cdrTime);
				log.debug(strdebug);
				return false; //���ڼ��ʱ�䷶Χ��
			}
			
			
			if (regtype != 1 && regtype != 3)
			{
				String strdebug = String.format("PowerUp�û�:%s״̬%d,START[%s] END[%s] CURRENT[%s]", 
						mobilenum,regtype,mobile.getStartTime(),mobile.getEndTime(),cdrTime);
				log.debug(strdebug);
				return false; //�����ϼ������
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
					action = "����";
					break;
				case 3:
					action = "�ػ�";
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s����%s��%s,%s",
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				String strdebug = String.format("CallNum�û�:%s���ڼ��ʱ�䷶Χ��,START[%s] END[%s] CURRENT[%s]", 
						mobilenum,mobile.getStartTime(),mobile.getEndTime(),cdrTime);
				log.debug(strdebug);
				return false; //���ڼ��ʱ�䷶Χ��
			}
			
			if(o_mobilenum.isEmpty() || !mobile.getCallNumList().contains(o_mobilenum))
			{//�����ض���Ա�����Ϣ
				String strdebug = String.format("CallNum�û�:%s %s�����ض���Ա�����Ϣ", 
						mobilenum,o_mobilenum);
				log.debug(strdebug);
				return false; //���ڼ��ʱ�䷶Χ��
			}
			
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "�ض�����û� -:" + " ��غ���:[" + mobilenum + "] ��ϵ����:[" + o_mobilenum + "]";
			
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s����%s��%s,%s",
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("ExitArea�û�:" +  mobilenum + " ���ڼ��ʱ�䷶Χ��!");
				return false; //���ڼ��ʱ�䷶Χ��
			}
			
			if(mobile.getExitAreaCellList().contains(cellname) || cellname.isEmpty())
			{//����
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "�û����뿪�������Ŀǰλ��["+ cellname +"]";
			
			
			
			
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s����%s��%s,%s",
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("EnterArea�û�:" +  mobilenum + " ���ڼ��ʱ�䷶Χ��!");
				return false; //���ڼ��ʱ�䷶Χ��
			}
			
			if(!mobile.getEnterAreaCellList().contains(cellname) || cellname.isEmpty())
			{//����
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "�û��ѽ���������Ŀǰλ��["+ cellname +"]";
			
					
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s����%s��%s,%s",
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("ExitLAC�û�:" +  mobilenum + " ���ڼ��ʱ�䷶Χ��!");
				return false; //���ڼ��ʱ�䷶Χ��
			}
		
			
			if(mobile.getExitLACList().containsKey(lac))
			{//����
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "�û����뿪�������ĿǰLAC["+ lac +"]";
			
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s����%s��%s,%s",
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("EnterLAC�û�:" +  mobilenum + " ���ڼ��ʱ�䷶Χ��!");
				return false; //���ڼ��ʱ�䷶Χ��
			}
			
			if(!mobile.getEnterLACList().containsKey(lac))
			{//����
				return false;
			}
					
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "�û��ѽ���������ĿǰLAC["+ lac +"]";
			
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s����%s��%s,%s",
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
				return false;//���Ǽ�غ���
			
			if(mobile.getStartTime().getTime() > cdrTime.getTime() 
					|| mobile.getEndTime().getTime() < cdrTime.getTime())
			{
				log.debug("������������-���ڼ��ʱ�䷶Χ��!");
				return false; //���ڼ��ʱ�䷶Χ��
			}
			
			if(!mobile.getEnterLACList().containsKey(lac)&&
					(!mobile.getEnterAreaCellList().contains(cellname) || cellname.isEmpty()))
			{//����
				return false;
			}
			
					
			//�û������ų���� ������
			if(mobile.getExcludeNumList()!=null && mobile.getExcludeNumList().containsKey(msisdn))
				return false;
			
			String monitor_name  = mobile.getMonitorName();
			String case_id = mobile.getCaseID();
			String hit_item =  mobile.getMonitorName();
			Date  hit_time = cdrTime;
			String cell_name = cellname;
			String action = "������[" + cityid + "]";
			
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
				//�������ݣ�
				//hit_time + case_id + "����" + monitor_name + "����" + misidn + "������" + cell_name + "�б꣬���ݣ�" + action 
				//����ͼ��һ��:2013-5-16 09:58:09 JN3421�����������Ų��Բ���15315115936������53122_21039_������԰�б꣬���ݣ����Ž���
				SimpleDateFormat formatter = new SimpleDateFormat("dd�� HH:mm:ss");
				String content = String.format("%s%s����%s��%s,%s",
						formatter.format(hit_time),monitor_name,msisdn,cell_name,action);
//				SendSMS sms = new SendSMS(sendlist,content);
//				sms.start();
			}
		}
		return true;
	}
	

}
