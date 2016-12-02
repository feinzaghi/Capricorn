package com.turk.specialapp;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.collect.FTPTool;

import com.turk.Config.SystemConfig;
import com.turk.util.CommonDB;
import com.turk.util.LogMgr;

/**
 * �����ɼ����
 * ΪGP���ݿ��ṩIDX�ļ��Ա㴴������
 * @author Administrator
 *
 */
public class CdrDayComplate {

	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private static CdrDayComplate _instance = null;
	
	private Map<String,CdrFileDayInfo> _fileInfoMap = new HashMap();
	
	public static CdrDayComplate getInstance()
	{
		if(_instance == null)
			_instance = new CdrDayComplate();
		return _instance;
	}
	
	public CdrDayComplate()
	{
		
	}
	
	public void ExcuteScan()
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			//log.debug("Starting getConnection...");
			conn = CommonDB.getConnection();
			//log.debug("GetConnection done...");
			if (conn == null)
			{
				log.error("��������ж�ȡ��Ϣʧ��,ԭ��:�޷���ȡ���ݿ�����.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = "";
			//��ȡԤ�������
			strSql = "select start_time,city_id,avg(case when filerate > 100 then 100 else filerate end) filerate from ("
				+" select bscid,city_id, start_time, filenum/288*100 as filerate"
				+" 	  from (select a.city_id,bscid,"
				+" 	               trunc(data_time,'DD') as start_time,"
				+" 	               count(*) as filenum"
				+" 	          from etl_mod_ea_log a,cfg_city b"
				+" 	         where a.city_id = b.city_id and b.vendor = 'ZY0808'"
				+" 	           and datatype = 'CDR'"
				+" 	           and a.ea_time > sysdate - 20"
				+" 	         group by a.city_id, bscid, trunc(data_time,'DD'))"
				+" 	 where (start_time,city_id) not in (select start_time,city_id from utl_data_cdrindex_log)"
				+" 	) group by start_Time,city_id";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		SimpleDateFormat ft1 = new SimpleDateFormat("yyyyMMdd");
	    		Date starttime = rs.getDate("START_TIME");
	    		int cityid = rs.getInt("CITY_ID");
	    		String key = ft1.format(starttime) + "_" + cityid;
	    		double filerate = rs.getDouble("FILERATE");
	    		if(filerate==100)
	    		{
	    			//����index�ļ������ϴ�
	    			CreateIndexFile(ft1.format(starttime),cityid);
	    			
	    			if(_fileInfoMap.containsKey(key))
	    			{
	    				_fileInfoMap.remove(key);
	    			}
	    		}
	    		else
	    		{
	    			if(_fileInfoMap.containsKey(key))
	    			{
	    				CdrFileDayInfo fileinfo = _fileInfoMap.get(key);
	    				Date stamptime = fileinfo.getStampTime();
	    				Date nowtime = new Date();
	    				//һСʱ�ж�һ��
	    				if(nowtime.getTime() - stamptime.getTime() < 3600*1000L)
	    					continue;
	    				
	    				if(filerate > 98 && fileinfo.getCount() == 1)
						{
							CreateIndexFile(ft1.format(starttime),cityid);
			    			if(_fileInfoMap.containsKey(key))
			    			{
			    				_fileInfoMap.remove(key);
			    			}
						}
	    				else if(filerate > 95 && fileinfo.getCount() == 2)
	    				{
	    					CreateIndexFile(ft1.format(starttime),cityid);
			    			if(_fileInfoMap.containsKey(key))
			    			{
			    				_fileInfoMap.remove(key);
			    			}
	    				}
	    				else if(filerate > 90 && fileinfo.getCount() >= 3)
	    				{
	    					CreateIndexFile(ft1.format(starttime),cityid);
			    			if(_fileInfoMap.containsKey(key))
			    			{
			    				_fileInfoMap.remove(key);
			    			}
	    				}
	    				else if(filerate > 85 && fileinfo.getCount() >= 10)
	    				{
	    					CreateIndexFile(ft1.format(starttime),cityid);
			    			if(_fileInfoMap.containsKey(key))
			    			{
			    				_fileInfoMap.remove(key);
			    			}
	    				}
	    				else if(filerate <= 85 && fileinfo.getCount() >= 30)
	    				{
	    					CreateIndexFile(ft1.format(starttime),cityid);
			    			if(_fileInfoMap.containsKey(key))
			    			{
			    				_fileInfoMap.remove(key);
			    			}
	    				}
	    				else
	    				{
	    					fileinfo.setStampTime(new Date());
	    					fileinfo.setCount(fileinfo.getCount()+1);
	    				}
	    			}
	    			else
	    			{
	    				Date nowtime = new Date();
	    				if(filerate > 88 || nowtime.getTime() - starttime.getTime() > 24 * 3600*1000L + 3 * 3600*1000L)
	    				{
		    				CdrFileDayInfo fileinfo = new CdrFileDayInfo();
		    				fileinfo.setCityID(cityid);
		    				fileinfo.setCount(1);
		    				fileinfo.setStampTime(new Date());
		    				fileinfo.setStartTime(starttime);
		    				_fileInfoMap.put(key, fileinfo);
	    				}
	    			}
	    		}
	    	}
	    	
		}catch (Exception e) {
				
				log.error("�����ʼ�����쳣:" + e.getMessage() ,e);
		}
		finally
		{
			CommonDB.close(rs, pstmt, conn);
		}
	}
	
	private boolean CreateIndexFile(String strDate,int nCityID)
	{
		boolean blResult = false;
		String logStr;
				
		String ftpIP = "172.168.0.253";
		String ftpuser = "gpftp";
		String ftppwd = "gpftp";
		
		if(nCityID == 20 || nCityID == 662 || nCityID == 668 || nCityID == 752 || nCityID == 760)
		{
			ftpIP = "172.168.0.101";
		}
		if(nCityID == 755 || nCityID == 757 || nCityID == 759 || nCityID == 769)
		{
			ftpIP = "172.168.0.101";
		}
		
		
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
		    
		    //String fileName1X = "cdr_hw_1x_" + nCityID + "_" + strDate + ".idx";
		    //String fileNameDO = "cdr_hw_do_" + nCityID + "_" + strDate + ".idx";
		    //String fileNameDOStream = "cdr_hw_dostream_" + nCityID + "_" + strDate + ".idx";
		    
		    String fileName1XV1 = "cdr_hw_1x_v1_" + nCityID + "_" + strDate + ".idx";
		    String fileNameDOV1 = "cdr_hw_do_v1_" + nCityID + "_" + strDate + ".idx";
		    String fileNameDOStreamV1 = "cdr_hw_dostream_v1_" + nCityID + "_" + strDate + ".idx";
		    
		    //File file1X = new File(SystemConfig.getInstance().getCurrentPath()
		    //		+ File.separatorChar + fileName1X);
		    //file1X.createNewFile();
		    
		    File file1XV1 = new File(SystemConfig.getInstance().getCurrentPath()
		    		+ File.separatorChar + fileName1XV1);
		    file1XV1.createNewFile();
		    
		    
		    //int code = ftp.uploadFile(file1X.getAbsolutePath(), docName);
		    int code = ftp.uploadFile(file1XV1.getAbsolutePath(), docName);
		    switch(code)
		    {
		    	case 100://�ɹ�
		    		//File sucfile = new File(fileName1X);
		    		//if(file1X.delete())
		    		//{
		    		//	log.debug("�ļ�:[" + fileName1X + " ]ɾ���ɹ�");
		    		//}
		    		if(file1XV1.delete())
		    		{
		    			log.debug("�ļ�:[" + fileName1XV1 + " ]ɾ���ɹ�");
		    		}
		    		
		    		//File fileDO = new File(SystemConfig.getInstance().getCurrentPath()
				    //		+ File.separatorChar + fileNameDO);
		    		//fileDO.createNewFile();
		    		
		    		File fileDOV1 = new File(SystemConfig.getInstance().getCurrentPath()
				    		+ File.separatorChar + fileNameDOV1);
		    		fileDOV1.createNewFile();
		    		
		    		//code = ftp.uploadFile(fileDO.getAbsolutePath(), docName);
		    		code = ftp.uploadFile(fileDOV1.getAbsolutePath(), docName);
		    		//if(fileDO.delete())
		    		//{
		    		//	log.debug("�ļ�:[" + fileNameDO + " ]ɾ���ɹ�");
		    		//}
		    		if(fileDOV1.delete())
		    		{
		    			log.debug("�ļ�:[" + fileNameDOV1 + " ]ɾ���ɹ�");
		    		}
		    		
		    		//File fileDOStream = new File(SystemConfig.getInstance().getCurrentPath()
				    //		+ File.separatorChar + fileNameDOStream);
		    		//fileDOStream.createNewFile();
		    		
		    		File fileDOStreamV1 = new File(SystemConfig.getInstance().getCurrentPath()
				    		+ File.separatorChar + fileNameDOStreamV1);
		    		fileDOStreamV1.createNewFile();
		    		
		    		//code = ftp.uploadFile(fileDOStream.getAbsolutePath(), docName);
		    		code = ftp.uploadFile(fileDOStreamV1.getAbsolutePath(), docName);
		    		//if(fileDOStream.delete())
		    		//{
		    		//	log.debug("�ļ�:[" + fileNameDOStream + " ]ɾ���ɹ�");
		    		//}
		    		if(fileDOStreamV1.delete())
		    		{
		    			log.debug("�ļ�:[" + fileNameDOStreamV1 + " ]ɾ���ɹ�");
		    		}
		    		
		    		//д����־
		    		InsertIndexLog(strDate,nCityID);
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
	
	private void InsertIndexLog(String strDate,int nCityID)
	{
		
		try
		{
			SimpleDateFormat ft1 = new SimpleDateFormat("yyyyMMdd");
			Date starttime = ft1.parse(strDate);
			SimpleDateFormat ft2 = new SimpleDateFormat("yyyy-MM-dd");
			String sTime = ft2.format(starttime);
			
			String strSql = String.format("INSERT INTO UTL_DATA_CDRINDEX_LOG (START_TIME,STAMPTIME,CITY_ID) " +
					"VALUES (TO_DATE('%s','YYYY-MM-DD'),SYSDATE,%d)", sTime,nCityID);
			CommonDB.executeUpdate(strSql);
	
		}catch (Exception e) {
				
				log.error("�����ʼ�����쳣:" + e.getMessage() ,e);
		}
		finally
		{
			//CommonDB.close(rs, pstmt, conn);
		}
	}
}
