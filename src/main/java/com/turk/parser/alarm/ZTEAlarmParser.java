package com.turk.parser.alarm;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import com.turk.parser.Parser;

import com.turk.templet.LineTempletP;
import com.turk.util.Util;

/**
 * ���˸澯����
 * @author Administrator
 *
 */
public class ZTEAlarmParser extends Parser{

	String remainingData = "";
	
	private Map<Integer,String> _cityInfo = new HashMap<Integer, String>();
	
	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		this.log.debug(this.collectObjInfo.getTaskID() + 
				":Start parser Para file��" + this.fileName);
	    File f = new File(this.fileName);
	   
	    if (!f.exists())
	    {
	    	this.log.error(this.collectObjInfo.getTaskID() + ":File does not exist��" + this.fileName);
	    	return false;
	    }
	    
	    //Init
	    _cityInfo.clear();
	    _cityInfo.put(762, "��Դ");
	    _cityInfo.put(768, "����");
	    _cityInfo.put(663, "����");
	    _cityInfo.put(763, "��Զ");
	    _cityInfo.put(753, "÷��");
	    _cityInfo.put(754, "��ͷ");
	    _cityInfo.put(660, "��β");
	    _cityInfo.put(751, "�ع�");
	    _cityInfo.put(766, "�Ƹ�");
	    _cityInfo.put(758, "����");
	    

	    FileInputStream fis = null;
  		try
  		{
  			String logStr = this + ": starting parse file : " + this.fileName;
  			this.log.debug(logStr);
  			this.collectObjInfo.log("����", logStr);
  			
  			File fs = new File(this.fileName);
      
  			if (!fs.exists())
  		    {
  		    	this.log.error(this.collectObjInfo.getTaskID() + ":File does not exist��" + this.fileName);
  		    	return false;
  		    }
  			
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
  			//�н���ģ��
  			LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();

  			LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
  				.get(0);
  			
  			StringBuffer strNewRow = new StringBuffer();
  			int city_id = 0;
  			String city_name = "";
  			String NodeId = "";
  			String BssId = "";
  			String BtsId = "";
  			String ALARM_ID = "";
  			String EVENT_TIME = "";
  			String NOTIFICATION_TYPE = "";
  			String MANAGED_OBJECT_INSTANCE = "";
  			String PERCEIVED_SEVERITY = "";
  			String ALARM_TYPE = "";
  			String PROBABLE_CAUSE = "";
  			String SPECIFIC_PROBLEM = "";
  			String ADDITIONAL_TEXT  = "";
  			for (int i = 0; i < nRowCount; i++)
  			{
  				
  				//������Ϊһ������
  				if (Util.isNull(strzRowData[i]))
  				{
  					try {
  						if(city_id==0||EVENT_TIME.isEmpty())
  							continue;
  						strNewRow.append(city_id + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(city_name + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(NodeId + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(BssId + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(BtsId + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(ALARM_ID + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(EVENT_TIME + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(NOTIFICATION_TYPE + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(MANAGED_OBJECT_INSTANCE + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(PERCEIVED_SEVERITY + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(ALARM_TYPE + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(PROBABLE_CAUSE + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(SPECIFIC_PROBLEM + subTemp.m_strNewFieldSplitSign);
  						strNewRow.append(ADDITIONAL_TEXT + subTemp.m_strNewFieldSplitSign);
  						
  						strNewRow.deleteCharAt(strNewRow.length()-1);//ȥ�����һ������
  						strNewRow.append("\n");
  						
  						this.distribute.DistributeData(strNewRow.toString().getBytes("UTF-8"), 0);
  					} catch (UnsupportedEncodingException e) {
  						// TODO Auto-generated catch block
  						log.error("ת��UTF-8��ʽ�쳣",e);
  					}
  					strNewRow = new StringBuffer();
  					continue;
  				}
  				String strOldRow = strzRowData[i];
  				
  				//city_id	��ȡNodeId=176601��2-4λ�����NODEID��ȡ766 MANAGED_OBJECT_INSTANCE
  				
  				//city_name	���ݳ���IDƥ���������
  				//NodeId	��ȡNodeId
  				//BssId	��ȡBssId
  				//BtsId	��ȡBtsId
  				strOldRow = strOldRow.replace("\r", "");
  				if(strOldRow.contains("MANAGED_OBJECT_INSTANCE"))
  				{
  					String[] objs = strOldRow.split(",",-1);
  					if(objs.length<3)
  						continue;
  					//MANAGED_OBJECT_INSTANCE=NodeId=176601,BssId=0,BtsId=622,RackId=1
  					NodeId = objs[0];
  					NodeId = NodeId.substring(NodeId.indexOf("NodeId="));
  					NodeId = NodeId.split("=",-1)[1];
  					
  					city_id = Integer.parseInt(NodeId.substring(1,4));
  					city_name = _cityInfo.get(city_id);
  					
  					BssId = objs[1];
  					BssId = BssId.split("=",-1)[1];
  					
  					BtsId = objs[2];
  					BtsId = BtsId.split("=",-1)[1];
  				}
  				
  				
  				//ALARM_ID	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("ALARM_ID"))
  				{
  					ALARM_ID = strOldRow.split("=",-1)[1];
  				}
  				//EVENT_TIME	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("EVENT_TIME"))
  				{
  					EVENT_TIME = strOldRow.split("=",-1)[1];
  				}
  				//NOTIFICATION_TYPE	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("NOTIFICATION_TYPE"))
  				{
  					NOTIFICATION_TYPE = strOldRow.split("=",-1)[1];
  				}
  				
  				//MANAGED_OBJECT_INSTANCE	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("MANAGED_OBJECT_INSTANCE"))
  				{
  					MANAGED_OBJECT_INSTANCE = strOldRow.substring(strOldRow.indexOf("NodeId=")).replace(",", ";");
  				}
  				
  				//PERCEIVED_SEVERITY	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("PERCEIVED_SEVERITY"))
  				{
  					PERCEIVED_SEVERITY = strOldRow.split("=",-1)[1];
  				}
  				
  				
  				//ALARM_TYPE	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("ALARM_TYPE"))
  				{
  					ALARM_TYPE = strOldRow.split("=",-1)[1];
  				}
  				
  				//PROBABLE_CAUSE	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("PROBABLE_CAUSE"))
  				{
  					PROBABLE_CAUSE = strOldRow.split("=",-1)[1];
  				}
  				
  				//SPECIFIC_PROBLEM	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("SPECIFIC_PROBLEM"))
  				{
  					SPECIFIC_PROBLEM = strOldRow.split("=",-1)[1];
  				}
  				
  				//ADDITIONAL_TEXT	ֱ�Ӷ�ȡ
  				if(strOldRow.contains("ADDITIONAL_TEXT"))
  				{
  					ADDITIONAL_TEXT = strOldRow.split("=",-1)[1].replace(",", ";");
  				}

  				
  			}
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

		StringBuffer strNewRow = new StringBuffer();
		String strValue = "";

		//����ɼ�ʱ��
		SimpleDateFormat spformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String strTime = spformat.format(this.collectObjInfo.getLastCollectTime());
		strNewRow.append(strTime + subTemp.m_strNewFieldSplitSign);
		

		try
		{
			
			
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
		strNewRow.deleteCharAt(strNewRow.length()-1);//ȥ�����һ������
		strNewRow.append("\n");
		
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
	
}
