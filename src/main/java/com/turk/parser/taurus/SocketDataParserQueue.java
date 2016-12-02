package com.turk.parser.taurus;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.turk.collect.SocketServer;
import com.turk.distributor.DistributeTemplet;
import com.turk.distributor.DistributeTemplet.TableTemplet;
import com.turk.task.CollectObjInfo;
import com.turk.templet.LineTempletP;
import com.turk.util.LogMgr;
import com.turk.util.Task;
import com.turk.util.Util;

/**
 * socket ���ݽ�������
 * @author Administrator
 *
 */
public class SocketDataParserQueue extends Task{
	
	private Logger log = LogMgr.getInstance().getSystemLogger();

	private SocketServer thisparser;
	private CollectObjInfo collectObjInfo = null;
	private boolean mStop = false;
	private int nCount = 0;
	int _CityID = 0;
	
	private HashMap<Integer,ScoketMsg> 
		msgMap = new HashMap<Integer, ScoketMsg>();
		
	/**
	* �ַ�ģ����
	*/
	int _DisTableID = -1;
		
	public SocketDataParserQueue(CollectObjInfo taskInfo,
				SocketServer parser)
	{
		this.collectObjInfo = taskInfo;
		this.thisparser = parser;
	}
	
	public void AddMsgMap(int MobileEventType,String data,String hexbody)
	{
		ScoketMsg msg = new ScoketMsg();
		msg.setData(data);
		msg.setMobileEventType(MobileEventType);
		msg.setHexbody(hexbody);
		this.msgMap.put(nCount++, msg);
	}
	
	public void run()
	{
		for(ScoketMsg msg: this.msgMap.values())
		{
			if(mStop)
				return;
			
			LoadData(msg.getData(),msg.getMoblieEventType(),msg.getHexbody());
		}
		this.msgMap.clear();
		//this.msgMap = null;
	}
	
	/**
	 * ���ݽ�������
	 * @param data		 
	 * @param MobileEventType
	 */
	public void LoadData(String data,int MobileEventType,
				String bodyhex)
	{
		String line;
		try {
	  		line = data;
  			
  			if (Util.isNull(line))
  			{
  				log.debug(MobileEventType + ": data is null");
  				return;
  			}
  			ParseLineData(MobileEventType,line,bodyhex);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Socket string load err",e);
		} 
	}
		
	
	/**
		 * 
		 * @param EventType  �����¼�����
		0�ƶ�Ѱ��PAGING
		1�ƶ�2G����CC
		2�ƶ�2G����SM
		3�ƶ�2Gλ�ø���MM
		4�ƶ�3G����CC
		5�ƶ�3G����SM
		6�ƶ�3Gλ�ø���MM
		7�ƶ�2G�л�HO
		8�ƶ�3G�л�RELOC
		10��ͨѰ��PAGING
		11��ͨ2G����CC
		12��ͨ2G����SM
		13��ͨ2Gλ�ø���MM
		14��ͨ3G����CC
		15��ͨ3G����SM
		16��ͨ3Gλ�ø���MM
		17��ͨ2G�л�HO
		18��ͨ3G�л�RELOC
		20����Ѱ��PAGING
		21���ź���CC
		22���Ŷ���SM
		23����λ�ø���MM
		 * @param strOldRow
		 */
		public void ParseLineData(int EventType,String strOldRow,
				String bodyhex)
		{
			int nSubTmpIndex = 0;
			//�н���ģ��
			LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
			String sEventType = "";
			//����ҵ������
			switch(EventType)
			{
				case 21:
					sEventType = "CC";
					break;
				case 22:
					sEventType = "SM";
					break;
				case 23:
					sEventType = "MM";
					break;
					
					
				case 1://1�ƶ�2G����CC
					sEventType = "CC_CMCC";
					break;
				case 2://2�ƶ�2G����SM
					sEventType = "SM_CMCC";
					break;
				case 3://3�ƶ�2Gλ�ø���MM
					sEventType = "MM_CMCC";
					break;
				case 4://4�ƶ�3G����CC
					sEventType = "RANAPCC_CMCC";
					break;
				case 5://5�ƶ�3G����SM
					sEventType = "RANAPSM_CMCC";
					break;
				case 6://6�ƶ�3Gλ�ø���MM
					sEventType = "RANAPMM_CMCC";
					break;
				case 7://7�ƶ�2G�л�HO
					sEventType = "HO_CMCC";
					break;
				case 8://8�ƶ�3G�л�RELOC
					sEventType = "RELOC_CMCC";
					break;
					
					
				case 11://11��ͨ2G����CC
					sEventType = "CC_UNICOM";
					break;
				case 12://12��ͨ2G����SM
					sEventType = "SM_UNICOM";
					break;
				case 13://13��ͨ2Gλ�ø���MM
					sEventType = "MM_UNICOM";
					//log.debug("MM_UNICOM:"+strOldRow);
					break;
				case 14://14��ͨ3G����CC
					sEventType = "RANAPCC_UNICOM";
					break;
				case 15://15��ͨ3G����SM
					sEventType = "RANAPSM_UNICOM";
					break;
				case 16://16��ͨ3Gλ�ø���MM
					sEventType = "RANAPMM_UNICOM";
					break;
				case 17://17��ͨ2G�л�HO
					sEventType = "HO_UNICOM";
					break;
				case 18://18��ͨ3G�л�RELOC
					sEventType = "RELOC_UNICOM";
					break;
					
				default :
					log.debug("Unkown MobileEventType:" + EventType + ":" + bodyhex);
					return;
			}

			//�Ƚϵõ��������ݵ�ģ��
			switch (templet.nScanType)
			{
				case 0:
				case 1:
					log.error("�н���ģ��ɨ���������ô��󣬴˴�Ӧ��Ϊ [2]");
					break;
				case 2:
				case 3:
					for (int i = 0; i < templet.m_nTemplet.size(); i++)
					{
						LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(i);
						String sTempTag = subTemp.m_tag;
						if (subTemp.m_nFileNameCompare == 0)
						{
							if (!logicEquals(sEventType, sTempTag))
								continue;
							nSubTmpIndex = i;
							break;
						}
						break;
					}
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
						this.log.warn("ParseType ���ô��󣬴˴�ӦΪ 5 ");
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
				String str = this + " : error when parsing data. templet name : " + 
					templet.tmpName + " data:" + strOldRow;
				this.log.error(str, e);
				this.collectObjInfo.log("����", str, e);
				return;
			}
			strNewRow.append(strValue);
			if(strNewRow.length()<=0)
				return;
			strNewRow.deleteCharAt(strNewRow.length()-1);//ȥ�����һ������
			strNewRow.append("\n");
			try {
				thisparser.distribute.DistributeData(strNewRow.toString().getBytes("UTF-8"), nSubTmpIndex);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				log.error("ת��UTF-8��ʽ�쳣",e);
			}
		}
		
		private boolean logicEquals(String shortFileName, String fileName)
		{
			if ((!fileName.contains("*")) && (!fileName.contains("?"))) 
				return shortFileName.equals(fileName);

			String s1 = shortFileName.replaceAll("\\.", "");
			String s2 = fileName.replaceAll("\\.", "");
			s1 = s1.replaceAll("\\+", "");
			s2 = s2.replaceAll("\\+", "");
			s2 = s2.replaceAll("\\*", ".*");
			s2 = s2.replaceAll("\\?", ".");

			return Pattern.matches(s2, s1);
		}
		
		private String ParseRowBySplit(LineTempletP.SubTemplet subTemp, 
				String strRow)
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
			
			if(subTemp.m_Filed.size()!=m_strTemp.length)
			{
				String strDebug = String.format("column count[%d] error:%s:%s"
						,m_strTemp.length, subTemp.m_tag, strRow);
				log.debug(strDebug);
				return "";
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
				if(_DisTableID == -10000)
					return "";
			}
			MonitorHit monitor = new MonitorHit("");
			TaurusParser parser = new TaurusParser(monitor,thisparser);
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
				sTableName = "cdr21_2";
				sColumnName = "START_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if(sValue!=null && !sValue.isEmpty())
				{
					_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
					
					if(sValue.equals("65535"))
						return -10000;
				}
				else
				{ 					
					log.debug("START_LAC:" + kv.get(sColumnName));
					_CityID = 531;
				}
			}
			else if(dataType.equals("CC_CMCC"))
			{
				//ѡ��ַ�ģ��
				sTableName = "cdr11_1";
				sColumnName = "START_LAC";
				sValue = kv.get(sColumnName);
				if(sValue!=null && !sValue.isEmpty())
				{
					if(sValue.equals("65535"))
						return -10000;
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
				sTableName = "cdr22_2";
				sColumnName = "START_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if(sValue!=null && !sValue.isEmpty())
				{
					_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
				}
				else
				{
					log.debug("START_LAC:" + kv.get(sColumnName));
					_CityID = 531;
				}
			}
			else if(dataType.equals("SM_CMCC"))
			{
				sTableName = "cdr12_1";
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
				sTableName = "cdr23_2";
				sColumnName = "DEST_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if(sValue!=null && !sValue.isEmpty())
				{
					_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
				}
				else
				{
					log.debug("START_LAC:" + kv.get(sColumnName));
					_CityID = 531;
				}
			}
			else if(dataType.equals("MM_CMCC"))
			{
				sTableName = "cdr13_1";
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
				sTableName = "cdr24_2";
				sColumnName = "START_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if(sValue!=null && !sValue.isEmpty())
				{
					_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
				}
				else
				{
					log.debug("START_LAC:" + kv.get(sColumnName));
					_CityID = 531;
				}
			}
			else if(dataType.equals("SCCP_CMCC"))
			{
				sTableName = "cdr14_1";
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
				sTableName = "cdr21_5";
				sColumnName = "START_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if(sValue!=null && !sValue.isEmpty())
				{
					_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
				}
				else
				{
					if(sValue.equals("65535"))
						return -10000;
					log.debug("START_LAC:" + kv.get(sColumnName));
					_CityID = 531;
				}
			}
			else if(dataType.equals("RANAPCC_CMCC"))
			{
				sTableName = "cdr11_4";
				sColumnName = "START_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if( sValue != null && !sValue.isEmpty())
				{
					//log.debug("MGW_IP:" + sValue);
					_CityID = MapLac2City.getInstance().getCityID(1,Integer.parseInt(sValue));
				}
				else
				{
					if(sValue.equals("65535"))
						return -10000;
					_CityID = 531;
				}
			}
			else if(dataType.equals("RANAPSM_UNICOM"))
			{
				sTableName = "cdr22_5";
				sColumnName = "START_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if(sValue!=null && !sValue.isEmpty())
				{
					_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
				}
				else
				{
					log.debug("START_LAC:" + kv.get(sColumnName));
					_CityID = 531;
				}
			}
			else if(dataType.equals("RANAPSM_CMCC"))
			{
				sTableName = "cdr12_4";
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
			else if(dataType.equals("RANAPMM_UNICOM"))
			{
				sTableName = "cdr23_5";
				sColumnName = "DEST_LAC";
				sValue = kv.get(sColumnName);
				sValue = Util.findByRegex(sValue, "[0-9]*", 0);
				if(sValue!=null && !sValue.isEmpty())
				{
					_CityID = MapLac2City.getInstance().getCityID(2,Integer.parseInt(sValue));
				}
				else
				{
					log.debug("START_LAC:" + kv.get(sColumnName));
					_CityID = 531;
				}
			}
			else if(dataType.equals("RANAPMM_CMCC"))
			{
				sTableName = "cdr13_4";
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
			DistributeTemplet disTemp = this.thisparser.distribute.getDisTemplet();
			for(int i = 0;i<disTemp.tableTemplets.values().size();i++)
			{
				TableTemplet disTable = disTemp.tableTemplets.get(i);
				if(disTable.tableName.equals(sCompare))
				{
					id = disTable.tableIndex;
					return id;
				}
				
			}
			return id;
		} 		
		
	@Override
	public String info() {
		// TODO Auto-generated method stub
		return "SocketDataParserQueue";
	}

	@Override
	protected boolean needExecuteImmediate() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void stopTask() {
		// TODO Auto-generated method stub
		this.mStop = true;
	}

	@Override
	public Task taskCore() throws Exception {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected boolean useDb() {
		// TODO Auto-generated method stub
		return false;
	}

	private String[] split(String linestr, String splitsign, String upsplitsign)
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
	
	private String removeNoiseSemicolon(String content)
	{
	    String strValue = content.replaceAll(";", " ");
	    strValue = content.replaceAll(",", " ");
	    return strValue;
	}
}
