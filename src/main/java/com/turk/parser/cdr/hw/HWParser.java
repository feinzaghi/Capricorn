package com.turk.parser.cdr.hw;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.turk.parser.Parser;
import com.turk.collect.FTPTool;
import com.turk.config.SystemConfig;
import com.turk.templet.LineTempletP;

/**
 * 华为话单解码
 * @author Administrator
 *
 */
public class HWParser extends Parser{

	//初始化三个需要做订阅备份的Filewrite对象
	private	FileOutputStream fw1x = null;
	private	FileOutputStream fwdo = null;
	private	FileOutputStream fwdostream = null;
		
		
	public static void main(String[] args)
	{
		try 
		{
			//HWParser parser = new HWParser();
	
			//parser.parseData();
			Date time1 = new Date(1386756104000L);
			Date time2 = new Date(1386756105000L);
			System.out.println(time1);
			System.out.println(time2);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("收到信息:" + strmsg);
		
	}
	
	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		//FileReader reader = null;
  		FileInputStream fis = null;
  		CommonFunc.InitCityInfo();
  		String strFileName1x = "";
		String strFileNamedo = "";
		String strFileNamedostream = "";
  		try
  		{
  			 			
  			//String logStr = this + ": starting parse file : " + this.fileName;
  			//this.log.debug(logStr);
  			//this.collectObjInfo.log("解析", logStr);

  			//this.fileName = "D:\\temp\\CHR_1_20121230103500.dat";
  			
  			Date start = new Date();
  			log.debug("CDR-TEST FILE:" + this.fileName);
  			log.debug("CDR-TEST START:" + start);
  			log.debug("开始解析[" + this.fileName + "]");
  			
  			
  			
  			if(SystemConfig.getInstance().isShare())
  			{
	  			SimpleDateFormat f = new SimpleDateFormat("yyyyMMddhhmmss");
	  			String sTime = f.format(this.collectObjInfo.getLastCollectTime());
	  	  		try
	  	  		{
	  	  			strFileName1x = SystemConfig.getInstance().getCurrentPath() + File.separatorChar + 
	  	  				String.format("cdr_hw_1x_%d_%d_%s_bak.txt", 
	  	  					this.collectObjInfo.getDevInfo().getCityID(),this.collectObjInfo.getTaskID(),
	  	  				sTime);
	  	  			
	  	  			strFileNamedo = SystemConfig.getInstance().getCurrentPath() + File.separatorChar +
	  	  				String.format("cdr_hw_do_%d_%d_%s_bak.txt", 
		  					this.collectObjInfo.getDevInfo().getCityID(),this.collectObjInfo.getTaskID(),
		  				sTime);
	  	  			
	  	  			strFileNamedostream = SystemConfig.getInstance().getCurrentPath() + File.separatorChar +
	  	  				String.format("cdr_hw_dostream_%d_%d_%s_bak.txt", 
	  					this.collectObjInfo.getDevInfo().getCityID(),this.collectObjInfo.getTaskID(),
	  					sTime);
	  	  			
	  	  			fw1x = new FileOutputStream(strFileName1x);
		  	  		fwdo = new FileOutputStream(strFileNamedo);
		  	  		fwdostream = new FileOutputStream(strFileNamedostream);
	  	  		}
	  	  		catch (IOException e)
	  	  		{
	  	  			String logStr = "error when building file head. ";
	  	  			this.log.error(logStr, e);
	  	  			//this.collectInfo.log("入库", logStr, e);
	  	  			return false;
	  	  		}
  			}
  			
  			File fs = new File(this.fileName);
      
  			fis = new FileInputStream(fs);
  			@SuppressWarnings("resource")
			DataInputStream dis = new DataInputStream(fis);
      
  			//long totalLength = fs.getTotalSpace();
  			
  			SDUCHRFILEHEAD sduhead = new SDUCHRFILEHEAD();
  			byte[] buffhead = new byte[36]; //文件头的长度
  			dis.read(buffhead);
  			sduhead.setBuf(buffhead);
  			
  			if(sduhead.dwFlag != 0xEFEFEFEF)
  			{
  				log.warn("文件不完整");
  				return false;
  			}
  			
  			for(int i=0;i<sduhead.dwMuNum;i++)
			{//处理每个MU测量单元的数据
  				byte[] buffMures = new byte[12];
  				dis.read(buffMures);
  				MuResultInfo resInfo = new MuResultInfo();
  				resInfo.setBuf(buffMures);
  				
  				//得到MI数量
  				int IgnoreByte = resInfo.MINum * 7;//忽略的字节数 MI数据不处理
  				byte[] buffIgnore = new byte[IgnoreByte];
  				dis.read(buffIgnore);
  				
  				MITypeVecInfo miinfo = new MITypeVecInfo();
  				miinfo.setBuf(buffIgnore,resInfo.MINum);
  				
  				//本单元的总字节数
  				int RecordBytes = resInfo.Mulength - IgnoreByte;
  				
  				//单行字节数
  				
  				if(resInfo.RecordNum == 0)
  				{
  					String strWarn = String.format("[TASKID:%d]-总记录数为0",
  							this.collectObjInfo.getTaskID());
  					log.warn(strWarn);
  					continue;
  				}
  				int RowBytes = RecordBytes / resInfo.RecordNum;
  				//开始处理话单记录

  				if(resInfo.MUID==2) //1X
				{
  					String strFileInfo = String.format("%d:1X--总字节数[%d] 总记录数[%d]", 
  	  						this.collectObjInfo.getTaskID(),RecordBytes,resInfo.RecordNum);
  	  				log.debug(strFileInfo);
  	  				
  					//行解析模版
  					LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
  					int nSubTmpIndex = -1;
  					switch (templet.nScanType)
  					{
  						case 0://通过FILENAME 标识的类型，对应相应的模版
  							for (int j = 0; j < templet.m_nTemplet.size(); j++)
  							{
  								LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(j);
  								if(subTemp.m_strFileName.equals("HW_CDR_1X"))
  								{
  									nSubTmpIndex = j;
  									break;
  								}
  							}
  							break;
  						default:
  							return false;
  					}
  			
  					LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
  						.get(nSubTmpIndex);
  					//写入字段名称
  					//StringBuffer strNewRow = new StringBuffer(); 
  					//for(int ii = 0;ii<subTemp.m_Filed.size();ii++)
  					//{
  					//	LineTempletP.FieldTemplet field = subTemp.m_Filed.get(ii); //获取模版的配置字段
  					//	strNewRow.append(field.m_strFieldName.toLowerCase() + subTemp.m_strNewFieldSplitSign);
  					//}
  					//strNewRow.append("longitude" + subTemp.m_strNewFieldSplitSign);
  					//strNewRow.append("latitude" + subTemp.m_strNewFieldSplitSign);
  					//strNewRow.append("\n");
  					//this.distribute.DistributeData(strNewRow.toString().getBytes(), nSubTmpIndex);
  					
	  				for(int ii = 0;ii < resInfo.RecordNum; ii++)
	  				{
		  				byte[] rowbyte = new byte[RowBytes];
		  				dis.read(rowbyte);
						Parse1XLine(rowbyte,miinfo,subTemp,nSubTmpIndex);
	  				}
  				}
  				
  				else if(resInfo.MUID==0x0003)//DO
  				{
  					String strFileInfo = String.format("%d:DO--总字节数[%d] 总记录数[%d]", 
  	  						this.collectObjInfo.getTaskID(),RecordBytes,resInfo.RecordNum);
  	  				log.debug(strFileInfo);
  	  				
  					//行解析模版
  					LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
  					int nSubTmpIndex = -1;
  					switch (templet.nScanType)
  					{
  						case 0://通过FILENAME 标识的类型，对应相应的模版
  							for (int j = 0; j < templet.m_nTemplet.size(); j++)
  							{
  								LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(j);
  								if(subTemp.m_strFileName.equals("HW_CDR_DO"))
  								{
  									nSubTmpIndex = j;
  									break;
  								}
  							}
  							break;
  						default:
  							return false;
  					}
  			
  					LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
  						.get(nSubTmpIndex);
  					
  					//写入字段名称
  					//StringBuffer strNewRow = new StringBuffer(); 
  					//for(int ii = 0;ii<subTemp.m_Filed.size();ii++)
  					//{
  					//	LineTempletP.FieldTemplet field = subTemp.m_Filed.get(ii); //获取模版的配置字段
  					//	strNewRow.append(field.m_strFieldName.toLowerCase() + subTemp.m_strNewFieldSplitSign);
  					//}
  					
  					//strNewRow.append("\n");
  					//this.distribute.DistributeData(strNewRow.toString().getBytes(), nSubTmpIndex);
  					
	  				for(int ii = 0;ii < resInfo.RecordNum; ii++)
	  				{
		  				byte[] rowbyte = new byte[RowBytes];
		  				dis.read(rowbyte);
		  				//ParseDOLine(rowbyte,miinfo,subTemp,nSubTmpIndex);
	  				}
  				}
  				else if(resInfo.MUID==0x0004) //DO Stream
  				{
  					String strFileInfo = String.format("%d:DO STREAM--总字节数[%d] 总记录数[%d]", 
  	  						this.collectObjInfo.getTaskID(),RecordBytes,resInfo.RecordNum);
  	  				log.debug(strFileInfo);
  					//行解析模版
  					LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
  					int nSubTmpIndex = -1;
  					switch (templet.nScanType)
  					{
  						case 0://通过FILENAME 标识的类型，对应相应的模版
  							for (int j = 0; j < templet.m_nTemplet.size(); j++)
  							{
  								LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet.get(j);
  								if(subTemp.m_strFileName.equals("HW_CDR_DOSTREAM"))
  								{
  									nSubTmpIndex = j;
  									break;
  								}
  							}
  							break;
  						default:
  							return false;
  					}
  			
  					LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
  						.get(nSubTmpIndex);
  					
  					//写入字段名称
  					//StringBuffer strNewRow = new StringBuffer(); 
  					///for(int ii = 0;ii<subTemp.m_Filed.size();ii++)
  					//{
  					//	LineTempletP.FieldTemplet field = subTemp.m_Filed.get(ii); //获取模版的配置字段
  					//	strNewRow.append(field.m_strFieldName.toLowerCase() + subTemp.m_strNewFieldSplitSign);
  					//}
  					//strNewRow.append("\n");
  					//this.distribute.DistributeData(strNewRow.toString().getBytes(), nSubTmpIndex);
  					
	  				for(int ii = 0;ii < resInfo.RecordNum; ii++)
	  				{
		  				byte[] rowbyte = new byte[RowBytes];
		  				dis.read(rowbyte);
		  				//ParseDOStreamLine(rowbyte,miinfo,subTemp,nSubTmpIndex);
	  				}
  				}
			}
  			log.debug("parse [" + this.fileName + "] complate");
  			Date end = new Date();
  			log.info("CDR-TEST END:" + end);
  			
  			
            long  mint=(end.getTime()-start.getTime())/(1000);   
             
  			log.info("CDR-TEST FINISH: Cost " + mint + " s");
  			
  			
  			
  		}
  		catch(Exception ex)
  		{
  			log.error("华为话单解析异常:[" + this.collectObjInfo.getTaskID() + "]",ex);
  		}
  		finally
  		{
  			try
  			{
  				if (fis != null) {
  					fis.close(); 
  				}
  				
  				if(SystemConfig.getInstance().isShare())
  				{
	  				if(fw1x!=null)
	  	  				fw1x.close();
	  	  			if(fwdo!=null)
	  	  				fwdo.close();
	  	  			if(fwdostream!=null)
	  	  				fwdostream.close();
	  	  			
	  	  			//上传备份
	  	  			SimpleDateFormat f = new SimpleDateFormat("yyyyMMddhhmmss");
	  	  			String sTime = f.format(this.collectObjInfo.getLastCollectTime().getTime()
	  	  					- 3*24*3600*1000L);
	  	  		
	  	  			String delele1x = String.format("cdr_hw_1x_%d_%d_%s_bak.txt", 
	  	  					this.collectObjInfo.getDevInfo().getCityID(),this.collectObjInfo.getTaskID(),
	  	  				sTime);
	  	  			String deleledo = String.format("cdr_hw_do_%d_%d_%s_bak.txt", 
  	  					this.collectObjInfo.getDevInfo().getCityID(),this.collectObjInfo.getTaskID(),
  	  				sTime);
	  	  			
	  	  			String deleledostream = String.format("cdr_hw_dostream_%d_%d_%s_bak.txt", 
  	  					this.collectObjInfo.getDevInfo().getCityID(),this.collectObjInfo.getTaskID(),
  	  				sTime);
	  	  			
	  	  			UploadBakFile(strFileName1x,delele1x);
	  	  			UploadBakFile(strFileNamedo,deleledo);
	  	  			UploadBakFile(strFileNamedostream,deleledostream);
  				}
  	  			
  			}
  			catch (Exception localException)
  			{
  			}
  		}
  		return true;
	}

	/**
	 * 1X 行解析
	 * @param rowbyte
	 * @param miinfo
	 * @param subTemp
	 * @param nSubTmpIndex
	 */
	private void Parse1XLine(byte[] rowbyte,MITypeVecInfo miinfo,
			LineTempletP.SubTemplet subTemp,int nSubTmpIndex)
	{
		try
		{
			StringBuffer strNewRow = new StringBuffer();
			String accesstime = "";
			String rowkey = "";
			int nStartpos = 0;
			
			Map<String,Object> 
				FieldValues = new HashMap<String, Object>();
			
			//strNewRow.append("{");
			
			for(int i = 0;i<subTemp.m_Filed.size();i++)
			{
				LineTempletP.FieldTemplet field = subTemp.m_Filed.get(i); //获取模版的配置字段
				MIInfo mi = miinfo.MIList[i];//解码后通过文件得到的字段信息
				byte[] temp = new byte[mi.MILength];
				System.arraycopy(rowbyte, nStartpos, temp, 0, temp.length);
				nStartpos = nStartpos + mi.MILength;//累加字节长度
				
				
				switch(mi.MIType)
				{
					case 0://数字类型
						switch(mi.MILength)
						{
							case 1: 
								int nValue1 = CommonFunc.unsignedByteToInt(temp[0]);
								if(field.m_dateFormat.equals("X"))
								{
									float fValue = ((float)nValue1 - (float)63)/(float)2;
									strNewRow.append(fValue + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValue1);
								}
								else
								{
									strNewRow.append(nValue1 + subTemp.m_strNewFieldSplitSign);
									//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaue1 + ",");
									FieldValues.put(field.m_strFieldName, nValue1);
								}
								break;
							case 2:
								short nValue2 = CommonFunc.getShort(temp,0);
								
								if(field.m_dateFormat.equals("X"))
								{
									float fValue = ((float)nValue2 - (float)63)/(float)2;
									strNewRow.append(fValue + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValue2);
								}
								else
								{
									strNewRow.append(nValue2 + subTemp.m_strNewFieldSplitSign);
									//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaue2 + ",");
									FieldValues.put(field.m_strFieldName, nValue2);
								}
								break;
							case 4:
								int nValaue4 = CommonFunc.bytesToInt(temp);
								if(field.m_type.equals("DATE"))
								{//时间戳字段315964800
									Date time = new Date(nValaue4*1000L + 315964800000L);
									SimpleDateFormat f = new SimpleDateFormat(field.m_dateFormat);
									String tValue = f.format(time);
									strNewRow.append(tValue + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValaue4);
									//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":{\"$date\":" + (nValaue4*1000L + 315964800000L) + "},");
									if(field.m_strFieldName.equals("ACCESS_TIME"))
										accesstime = tValue;
									break;
								}
								if(field.m_type.equals("HEX16"))
								{
									String sValue16 = CommonFunc.bytesToHexString(temp);
									strNewRow.append(sValue16 + subTemp.m_strNewFieldSplitSign);
									//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":\"" + sValue16 + "\",");
									FieldValues.put(field.m_strFieldName, sValue16);
									break;
								}
								strNewRow.append(nValaue4 + subTemp.m_strNewFieldSplitSign);
								//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaue4 + ",");
								FieldValues.put(field.m_strFieldName, nValaue4);
								break;
							default:
								int nValaueN = CommonFunc.bytesToInt(temp);
								strNewRow.append(nValaueN + subTemp.m_strNewFieldSplitSign);
								//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaueN + ",");
								FieldValues.put(field.m_strFieldName, nValaueN);
								break;
						}
						break;
					case 1://十六进制
						String sValue16 = CommonFunc.bytesToHexString(temp);
						strNewRow.append(sValue16 + subTemp.m_strNewFieldSplitSign);
						//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":\"" + sValue16 + "\",");
						FieldValues.put(field.m_strFieldName, sValue16);
						break;
					case 2://字符串
						String sValue = new String(temp);
						strNewRow.append(sValue.trim() + subTemp.m_strNewFieldSplitSign);
						//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":\"" + sValue + "\",");
						FieldValues.put(field.m_strFieldName, sValue);
						break;
					default:
						//strNewRow.append("\"MITYPE\":\"" + mi.MIType + "\",");
						strNewRow.append("MITYPE:[" + mi.MIType + "]" + subTemp.m_strNewFieldSplitSign);
						break;
				}
			}
			

			//接入小区经纬度
			double access_longitude = 0;
			double access_latitude = 0;
			
			//释放小区经纬度
			double release_longitude = 0;
			double release_latitude = 0;
			
			//定位计算
			int access_cell = 0;
			int access_sector = 0;
			int access_channel = 0;
			
			int last_conn_cell = 0;
			int last_conn_sector = 0;
			
			int bsc_id = Integer.parseInt(String.valueOf(this.collectObjInfo.getTaskID())
				.replace(String.valueOf(this.collectObjInfo.getDevInfo().getDevID()), ""));
			int nFirstPNCount = 0;
			//---用于定位---
			access_cell = Integer.parseInt(FieldValues.get("ACCESS_CELL").toString());
			access_sector = Integer.parseInt(FieldValues.get("ACCESS_SECTOR").toString());
			access_channel = Integer.parseInt(FieldValues.get("ASSIGN_CHANNEL").toString());
			nFirstPNCount = Integer.parseInt(FieldValues.get("FIRST_PSMM_PN_COUNT").toString());
			
			last_conn_cell = Integer.parseInt(FieldValues.get("LAST_RF_CONN1_CELL").toString());
			last_conn_sector = Integer.parseInt(FieldValues.get("LAST_RF_CONN1_SECTOR").toString());
			
			
			int[] m_nPsmmCell = new int[6]; //导频测量的小区编号
			int[] m_nPsmmSector = new int[6];//导频测量的小区编号
			int[] m_nPsmmDelay = new int[6]; //导频延迟
			int	m_nPsmmNum;
			String	m_strKey;//记录计算后得到的经纬度
			String nekey = bsc_id + "_" + access_cell + "_" + access_sector + "_" + access_channel;
			NEInfo ne = NEConfig.getInstance(this.getCollectObjInfo().getDevInfo().getCityID())
				.getNEInfo(this.getCollectObjInfo().getDevInfo().getCityID(), nekey);
			
			
			//rowkey time_imsi_dialeddigits_bsc_accesscell
			rowkey = String.format("%s_%s_%s_%d_%s", accesstime,
					FieldValues.get("IMSI").toString(),
					FieldValues.get("DIALED_DIGITS").toString(),
					bsc_id,
					FieldValues.get("ACCESS_CELL").toString());
		
			if(ne!=null)
			{
				try
				{
					//主小区可以找到，进行计算
					//导频个数
					m_nPsmmNum = nFirstPNCount;
					if(m_nPsmmNum>6)
					{
						m_nPsmmNum = 6;
					}
					for(int nP=0;nP<m_nPsmmNum;nP++) //定位：用FIRST_PSMM
					{
						m_strKey = String.format("FIRST_PSMM_CELL%d",nP +1);
						m_nPsmmCell[nP] = Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("FIRST_PSMM_SECTOR%d",nP +1);
						m_nPsmmSector[nP]  = Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("FIRST_PSMM_PN%d_ONEWAYDELAY",nP +1);
						m_nPsmmDelay[nP] = Integer.parseInt(FieldValues.get(m_strKey).toString());
	
					}
				
					int nAccessOneWayDelay = Integer.parseInt(FieldValues.get("SERVICE_ONE_WAY_DELAY").toString());
					//已主小区开始为基础计算
					CCalLocbyDis calloc = new CCalLocbyDis();
					//NEInfo ne,int nDelay,boolean flag,int m_nPsmmNum,int[] m_nPsmmCell,int[] m_nPsmmSector
					TDoublePoint point = calloc.CalLatAndLon(ne,nAccessOneWayDelay,true,m_nPsmmNum,
							m_nPsmmCell,m_nPsmmSector);
					if(point!=null)
					{
						access_longitude = point.x;
						access_latitude = point.y;
					}
				}
				catch(Exception ex)
				{
					log.error("话单定位异常",ex);
				}
			}
			
			String releasenekey = bsc_id + "_" + last_conn_cell + "_" + last_conn_sector + "_" + access_channel;
			NEInfo releasene = NEConfig.getInstance(this.getCollectObjInfo().getDevInfo().getCityID())
				.getNEInfo(this.getCollectObjInfo().getDevInfo().getCityID(), releasenekey);
			if(releasene!=null)
			{
				try
				{
					//主小区可以找到，进行计算
					//导频个数
					m_nPsmmNum = nFirstPNCount;
					if(m_nPsmmNum>6)
					{
						m_nPsmmNum = 6;
					}
					for(int nP=0;nP<2;nP++) //定位：用FIRST_PSMM
					{
						m_strKey = String.format("LAST_RF_CONN%d",nP +2);
						m_nPsmmCell[nP] = FieldValues.get(m_strKey)==null?0:Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("LAST_RF_CONN%d",nP +2);
						m_nPsmmSector[nP]  = FieldValues.get(m_strKey)==null?0:Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("LAST_RF_CONN%d_ONEWAYDELAY",nP +2);
						m_nPsmmDelay[nP] = FieldValues.get(m_strKey)==null?0:Integer.parseInt(FieldValues.get(m_strKey).toString());
	
					}
				
					int nAccessOneWayDelay = Integer.parseInt(FieldValues.get("LAST_RF_CONN1_ONEWAYDELAY").toString());
					//已主小区开始为基础计算
					CCalLocbyDis calloc = new CCalLocbyDis();
					//NEInfo ne,int nDelay,boolean flag,int m_nPsmmNum,int[] m_nPsmmCell,int[] m_nPsmmSector
					TDoublePoint point = calloc.CalLatAndLon(ne,nAccessOneWayDelay,true,m_nPsmmNum,
							m_nPsmmCell,m_nPsmmSector);
					if(point!=null)
					{
						release_longitude = point.x;
						release_latitude = point.y;
					}
				}
				catch(Exception ex)
				{
					log.error("话单定位异常",ex);
				}
			}
			
			strNewRow.append(bsc_id + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(access_longitude + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(access_latitude + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(release_longitude + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(release_latitude + subTemp.m_strNewFieldSplitSign);
			
			int access_time = Integer.parseInt(FieldValues.get("ACCESS_TIME").toString());
			int call_duration =  Integer.parseInt(FieldValues.get("CALL_DURATION").toString());

			SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date release_time = new Date((access_time + (call_duration/1000 ))*1000L + 315964800000L);
			String strReleaseTime = f.format(release_time);
			
			strNewRow.append(strReleaseTime + subTemp.m_strNewFieldSplitSign);
			strNewRow.append("v1.1.0");
			
			if(SystemConfig.getInstance().isShare() && fw1x!=null)
			{
				fw1x.write((strNewRow.toString() + "\n").getBytes());
				fw1x.flush();
			}
			
			//String strALL = strNewRow.toString().replace(subTemp.m_strNewFieldSplitSign, "|");
			//strNewRow.append(rowkey+"|"+strALL);
			//strNewRow.append("}");
			StringBuffer strWrite = new StringBuffer();
			strWrite.append(rowkey + subTemp.m_strNewFieldSplitSign);
			strWrite.append(strNewRow);
			strWrite.append("\n");
			
			this.distribute.DistributeData(strWrite.toString().getBytes(), nSubTmpIndex);
		}
		catch(Exception ex)
		{
			String msgerr = String.format("话单解码异常");
			log.error(msgerr,ex);
		}
	}
	
	/**
	 * DO 行解析
	 * @param rowbyte
	 * @param miinfo
	 * @param subTemp
	 * @param nSubTmpIndex
	 */
	private void ParseDOLine(byte[] rowbyte,MITypeVecInfo miinfo,
			LineTempletP.SubTemplet subTemp,int nSubTmpIndex)
	{
		try
		{
			StringBuffer strNewRow = new StringBuffer();
			int nStartpos = 0;
			//strNewRow.append("{");
			
			Map<String,Object> 
				FieldValues = new HashMap<String, Object>();
			
			for(int i = 0;i<subTemp.m_Filed.size();i++)
			{
				LineTempletP.FieldTemplet field = subTemp.m_Filed.get(i); //获取模版的配置字段
				MIInfo mi = miinfo.MIList[i];//解码后通过文件得到的字段信息
				byte[] temp = new byte[mi.MILength];
				System.arraycopy(rowbyte, nStartpos, temp, 0, temp.length);
				nStartpos = nStartpos + mi.MILength;//累加字节长度
				
			
				switch(mi.MIType)
				{
				case 0://数字类型
					switch(mi.MILength)
					{
						case 1: 
							int nValue1 = CommonFunc.unsignedByteToInt(temp[0]);
							if(field.m_dateFormat.equals("X"))
							{
								float fValue = ((float)nValue1 - (float)63)/(float)2;
								strNewRow.append(fValue + subTemp.m_strNewFieldSplitSign);
								FieldValues.put(field.m_strFieldName, nValue1);
							}
							else
							{
								strNewRow.append(nValue1 + subTemp.m_strNewFieldSplitSign);
								//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaue1 + ",");
								FieldValues.put(field.m_strFieldName, nValue1);
							}
							break;
						case 2:
							short nValue2 = CommonFunc.getShort(temp,0);
							if(field.m_dateFormat.equals("X"))
							{
								float fValue = ((float)nValue2 - (float)63)/(float)2;
								strNewRow.append(fValue + subTemp.m_strNewFieldSplitSign);
								FieldValues.put(field.m_strFieldName, nValue2);
							}
							else
							{
								strNewRow.append(nValue2 + subTemp.m_strNewFieldSplitSign);
								//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaue2 + ",");
								FieldValues.put(field.m_strFieldName, nValue2);
							}
							break;
						case 4:
							int nValue4 = CommonFunc.bytesToInt(temp);
							if(field.m_type.equals("DATE"))
							{//时间戳字段315964800
								Date time = new Date(nValue4*1000L + 315964800000L);
								SimpleDateFormat f = new SimpleDateFormat(field.m_dateFormat);
								String tValue = f.format(time);
								strNewRow.append(tValue + subTemp.m_strNewFieldSplitSign);
								//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":{\"$date\":" + (nValaue4*1000L + 315964800000L) + "},");
								FieldValues.put(field.m_strFieldName, nValue4);
								break;
							}
							if(field.m_type.equals("HEX16"))
							{
								String sValue16 = CommonFunc.bytesToHexString(temp);
								strNewRow.append(sValue16 + subTemp.m_strNewFieldSplitSign);
								//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":\"" + sValue16 + "\",");
								FieldValues.put(field.m_strFieldName, sValue16);
								break;
							}
							FieldValues.put(field.m_strFieldName, nValue4);
							strNewRow.append(nValue4 + subTemp.m_strNewFieldSplitSign);
							//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaue4 + ",");
							
							break;
						default:
							int nValaueN = CommonFunc.bytesToInt(temp);
							strNewRow.append(nValaueN + subTemp.m_strNewFieldSplitSign);
							//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":" + nValaueN + ",");
							FieldValues.put(field.m_strFieldName, nValaueN);
							break;
					}
					break;
				case 1://十六进制
					String sValue16 = CommonFunc.bytesToHexString(temp);
					strNewRow.append(sValue16 + subTemp.m_strNewFieldSplitSign);
					//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":\"" + sValue16 + "\",");
					FieldValues.put(field.m_strFieldName, sValue16);
					break;
				case 2://字符串
					String sValue = new String(temp);
					strNewRow.append(sValue.trim() + subTemp.m_strNewFieldSplitSign);
					//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":\"" + sValue + "\",");
					FieldValues.put(field.m_strFieldName, sValue);
					break;
				default:
					//strNewRow.append("\"MITYPE\":\"" + mi.MIType + "\",");
					strNewRow.append("MITYPE:[" + mi.MIType + "]" + subTemp.m_strNewFieldSplitSign);
					break;
				}
			}
			
			//接入小区经纬度
			double access_longitude = 0;
			double access_latitude = 0;
			
			//释放小区经纬度
			double release_longitude = 0;
			double release_latitude = 0;
			
			//定位计算
			int access_cell = 0;
			int access_sector = 0;
			int access_channel = 0;
			
			int last_conn_cell = 0;
			int last_conn_sector = 0;
			
			int bsc_id = Integer.parseInt(String.valueOf(this.collectObjInfo.getTaskID())
				.replace(String.valueOf(this.collectObjInfo.getDevInfo().getDevID()), ""));
			int nFirstPNCount = 0;
			//---用于定位---
			access_cell = Integer.parseInt(FieldValues.get("ACCESS_CELL").toString());
			access_sector = Integer.parseInt(FieldValues.get("ACCESS_SECTOR").toString());
			access_channel = Integer.parseInt(FieldValues.get("ASSIGN_CHANNEL").toString());
			nFirstPNCount = Integer.parseInt(FieldValues.get("FIRST_PSMM_PN_COUNT").toString());
			
			last_conn_cell = Integer.parseInt(FieldValues.get("LAST_RF_CONN1_CELL").toString());
			last_conn_sector = Integer.parseInt(FieldValues.get("LAST_RF_CONN1_SECTOR").toString());
			
			
			int[] m_nPsmmCell = new int[6]; //导频测量的小区编号
			int[] m_nPsmmSector = new int[6];//导频测量的小区编号
			int[] m_nPsmmDelay = new int[6]; //导频延迟
			int	m_nPsmmNum;
			String	m_strKey;//记录计算后得到的经纬度
			String nekey = bsc_id + "_" + access_cell + "_" + access_sector + "_" + access_channel;
			NEInfo ne = NEConfig.getInstance(this.getCollectObjInfo().getDevInfo().getCityID())
				.getNEInfo(this.getCollectObjInfo().getDevInfo().getCityID(), nekey);
			
		
			if(ne!=null)
			{
				try
				{
					//主小区可以找到，进行计算
					//导频个数
					m_nPsmmNum = nFirstPNCount;
					if(m_nPsmmNum>6)
					{
						m_nPsmmNum = 6;
					}
					for(int nP=0;nP<m_nPsmmNum;nP++) //定位：用FIRST_PSMM
					{
						m_strKey = String.format("FIRST_PSMM_CELL%d",nP + 1);
						m_nPsmmCell[nP] = Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("FIRST_PSMM_SECTOR%d",nP + 1);
						m_nPsmmSector[nP]  = Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("FIRST_PSMM_PN%d_ONEWAYDELAY",nP + 1);
						m_nPsmmDelay[nP] = Integer.parseInt(FieldValues.get(m_strKey).toString());
	
					}
				
					int nAccessOneWayDelay = Integer.parseInt(FieldValues.get("FIRST_PSMM_PN1_ONEWAYDELAY").toString());
					//已主小区开始为基础计算
					CCalLocbyDis calloc = new CCalLocbyDis();
					//NEInfo ne,int nDelay,boolean flag,int m_nPsmmNum,int[] m_nPsmmCell,int[] m_nPsmmSector
					TDoublePoint point = calloc.CalLatAndLon(ne,nAccessOneWayDelay,true,m_nPsmmNum,
							m_nPsmmCell,m_nPsmmSector);
					if(point!=null)
					{
						access_longitude = point.x;
						access_latitude = point.y;
					}
				}
				catch(Exception ex)
				{
					log.error("话单定位异常",ex);
				}
			}
			
			String releasenekey = bsc_id + "_" + last_conn_cell + "_" + last_conn_sector + "_" + access_channel;
			NEInfo releasene = NEConfig.getInstance(this.getCollectObjInfo().getDevInfo().getCityID())
				.getNEInfo(this.getCollectObjInfo().getDevInfo().getCityID(), releasenekey);
			if(releasene!=null)
			{
				try
				{
					//主小区可以找到，进行计算
					//导频个数
					m_nPsmmNum = nFirstPNCount;
					if(m_nPsmmNum>6)
					{
						m_nPsmmNum = 6;
					}
					for(int nP=0;nP<2;nP++) //定位：用FIRST_PSMM
					{
						m_strKey = String.format("LAST_RF_CONN%d_CELL",nP +2);
						if(FieldValues.get(m_strKey) == null)
						{
							log.debug("stop");
						}
						m_nPsmmCell[nP] = Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("LAST_RF_CONN%d_SECTOR",nP +2);
						m_nPsmmSector[nP]  = Integer.parseInt(FieldValues.get(m_strKey).toString());
						m_strKey = String.format("LAST_RF_CONN%d_ONEWAYDELAY",nP +2);
						m_nPsmmDelay[nP] = Integer.parseInt(FieldValues.get(m_strKey).toString());
	
					}
				
					int nAccessOneWayDelay = Integer.parseInt(FieldValues.get("LAST_RF_CONN1_ONEWAYDELAY").toString());
					//已主小区开始为基础计算
					CCalLocbyDis calloc = new CCalLocbyDis();
					//NEInfo ne,int nDelay,boolean flag,int m_nPsmmNum,int[] m_nPsmmCell,int[] m_nPsmmSector
					TDoublePoint point = calloc.CalLatAndLon(ne,nAccessOneWayDelay,true,m_nPsmmNum,
							m_nPsmmCell,m_nPsmmSector);
					if(point!=null)
					{
						release_longitude = point.x;
						release_latitude = point.y;
					}
				}
				catch(Exception ex)
				{
					log.error("话单定位异常",ex);
				}
			}
			
			strNewRow.append(bsc_id + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(access_longitude + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(access_latitude + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(release_longitude + subTemp.m_strNewFieldSplitSign);
			strNewRow.append(release_latitude + subTemp.m_strNewFieldSplitSign);
			
			int access_time = Integer.parseInt(FieldValues.get("ACCESS_TIME").toString());
			int call_duration =  Integer.parseInt(FieldValues.get("CALL_DURATION").toString());

			SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			Date release_time = new Date((access_time + (call_duration/1000 ))*1000L + 315964800000L);
			String strReleaseTime = f.format(release_time);
			
			strNewRow.append(strReleaseTime + subTemp.m_strNewFieldSplitSign);
			
			strNewRow.append("v1.1.0" + subTemp.m_strNewFieldSplitSign);
			//strNewRow.substring(0, strNewRow.lastIndexOf(","));
			//strNewRow.append("\"\":\"\"}");
			
			if(SystemConfig.getInstance().isShare() && fwdo!=null)
			{
				fwdo.write((strNewRow.toString() + "\n").getBytes());
				fwdo.flush();
			}
			
			String strALL = strNewRow.toString().replace(subTemp.m_strNewFieldSplitSign, "|");
			strNewRow.append(strALL);
			strNewRow.append("\n");
			this.distribute.DistributeData(strNewRow.toString().getBytes(), nSubTmpIndex);
		}
		catch(Exception ex)
		{
			String msgerr = String.format("话单解码异常");
			log.error(msgerr,ex);
		}
	}
	
	
	/**
	 * DOStream 行解析
	 * @param rowbyte
	 * @param miinfo
	 * @param subTemp
	 * @param nSubTmpIndex
	 */
	private void ParseDOStreamLine(byte[] rowbyte,MITypeVecInfo miinfo,
			LineTempletP.SubTemplet subTemp,int nSubTmpIndex)
	{
		try
		{
			StringBuffer strNewRow = new StringBuffer();
			int nStartpos = 0;
			
			Map<String,Object> 
				FieldValues = new HashMap<String, Object>();
			
			for(int i = 0;i<subTemp.m_Filed.size();i++)
			{
				LineTempletP.FieldTemplet field = subTemp.m_Filed.get(i); //获取模版的配置字段
				MIInfo mi = miinfo.MIList[i];//解码后通过文件得到的字段信息
				byte[] temp = new byte[mi.MILength];
				System.arraycopy(rowbyte, nStartpos, temp, 0, temp.length);
				nStartpos = nStartpos + mi.MILength;//累加字节长度
				
				
				switch(mi.MIType)
				{
					case 0://数字类型
						switch(mi.MILength)
						{
							case 1: 
								int nValue1 = CommonFunc.unsignedByteToInt(temp[0]);
								if(field.m_dateFormat.equals("X"))
								{
									float fValue = ((float)nValue1 - (float)63)/(float)2;
									strNewRow.append(fValue + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValue1);
								}
								else
								{
									strNewRow.append(nValue1 + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValue1);
								}
								break;
							case 2:
								short nValue2 = CommonFunc.getShort(temp,0);
								if(field.m_dateFormat.equals("X"))
								{
									float fValue = ((float)nValue2 - (float)63)/(float)2;
									strNewRow.append(fValue + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValue2);
								}
								else
								{
									strNewRow.append(nValue2 + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValue2);
								}
								break;
							case 4:
								int nValue4 = CommonFunc.bytesToInt(temp);
								if(field.m_type.equals("DATE"))
								{//时间戳字段
									Date time = new Date(nValue4*1000L + 315964800000L);
									SimpleDateFormat f = new SimpleDateFormat(field.m_dateFormat);
									String tValue = f.format(time);
									strNewRow.append(tValue + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, tValue);
									break;
								}
								if(field.m_type.equals("HEX16"))
								{
									String sValue16 = CommonFunc.bytesToHexString(temp);
									strNewRow.append(sValue16 + subTemp.m_strNewFieldSplitSign);
									//strNewRow.append("\"" + field.m_strFieldName.toLowerCase() + "\":\"" + sValue16 + "\",");
									FieldValues.put(field.m_strFieldName, sValue16);
									break;
								}
								
								if(field.m_dateFormat.equals("X"))
								{
									float fValue = ((float)nValue4 - (float)63)/(float)2;
									strNewRow.append(fValue + subTemp.m_strNewFieldSplitSign);
									FieldValues.put(field.m_strFieldName, nValue4);
								}
								else
								{
									FieldValues.put(field.m_strFieldName, nValue4);
									strNewRow.append(nValue4 + subTemp.m_strNewFieldSplitSign);
								}
								break;
							default:
								int nValaueN = CommonFunc.bytesToInt(temp);
								FieldValues.put(field.m_strFieldName, nValaueN);
								strNewRow.append(nValaueN + subTemp.m_strNewFieldSplitSign);
								break;
						}
						break;
					case 1://十六进制
						String sValue16 = CommonFunc.bytesToHexString(temp);
						FieldValues.put(field.m_strFieldName, sValue16);
						strNewRow.append(sValue16 + subTemp.m_strNewFieldSplitSign);
						break;
					case 2://字符串
						String sValue = new String(temp);
						FieldValues.put(field.m_strFieldName, sValue);
						strNewRow.append(sValue + subTemp.m_strNewFieldSplitSign);
						break;
					default:
						strNewRow.append("MITYPE:[" + mi.MIType + "]" + subTemp.m_strNewFieldSplitSign);
						break;
				}
			}
			
			//DistributeTemplet.TableTemplet 
			//	temp = (DistributeTemplet.TableTemplet)this.distribute.getDisTemplet().tableTemplets.get(2);
			//for(int i = 0;i < temp.fields.size();i++)
			//{
			//	DistributeTemplet.FieldTemplet disField = temp.fields.get(i);
			//	strNewRow.append(FieldValues.get(disField.m_strFieldName) + subTemp.m_strNewFieldSplitSign);
			//}
			
			int bsc_id = Integer.parseInt(String.valueOf(this.collectObjInfo.getTaskID())
					.replace(String.valueOf(this.collectObjInfo.getDevInfo().getDevID()), ""));
			
			strNewRow.append(bsc_id + subTemp.m_strNewFieldSplitSign);
			strNewRow.append("v1.1.0" + subTemp.m_strNewFieldSplitSign);
			//strNewRow.substring(0, strNewRow.length() -1);
			if(SystemConfig.getInstance().isShare() && fwdostream!=null)
			{
				fwdostream.write((strNewRow.toString() + "\n").getBytes());
				fwdostream.flush();
			}
			
			String strALL = strNewRow.toString().replace(subTemp.m_strNewFieldSplitSign, "|");
			strNewRow.append(strALL);
			strNewRow.append("\n");
			this.distribute.DistributeData(strNewRow.toString().getBytes(), nSubTmpIndex);
		}
		catch(Exception ex)
		{
			String msgerr = String.format("DOStream话单解码异常");
			log.error(msgerr,ex);
		}
	}
	
	private void UploadBakFile(String filepath,String deletefile)
	{
		String logStr = "";
		///662/CDR/YJBSC1/export/
		String uploadPath = String.format("/%d/CDR/%s/export/", this.collectObjInfo.getDevInfo().getCityID(),
				this.collectObjInfo.getDescribe());
		
		
		String ftpIP = SystemConfig.getInstance().getIp();
		String ftpuser = SystemConfig.getInstance().getUser();
		String ftppwd = SystemConfig.getInstance().getPwd();
		FTPTool ftp = new FTPTool(ftpIP,21,ftpuser,ftppwd);
			
		//已当前采集任务描述作为采集目录
		ftp.setKeyID(String.valueOf(this.collectObjInfo.getTaskID()));
			
			
		logStr = "分发数据：开始FTP登陆.";
		this.log.debug(logStr);
		try
			{
				boolean bOK = ftp.login(30000, 5);
				if (!bOK)
			 	{
					logStr = "分发数据: FTP多次尝试登陆失败:" + ftp;
					this.log.error(logStr);
					return;
			 	}
			    logStr = "分发数据: FTP登陆成功.";
			    this.log.debug("分发数据: FTP登陆成功.");
			    
			    String fileName = filepath;
			    
			    
			    int code = ftp.uploadFile(fileName, uploadPath);
			    
			    switch(code)
			    {
			    	case 100://成功
			    		//删除FTP中3天前的文件
			    		boolean delResult = ftp.DeleteFile(uploadPath + deletefile);
			    		logStr = "删除FTP上的文件:"+delResult + ":" + uploadPath + deletefile;
				    	this.log.debug(logStr);
			    		break;
			    	case 400:
			    		//异常
			    		logStr = "400 上传备份目录异常:"+fileName;
				    	this.log.debug(logStr);
			    		break;
			    	case 401:
			    		//三次重试失败
			    		logStr = "401 上传备份目录异常:"+fileName;
				    	this.log.debug(logStr);
			    		break;
			    }
			    File sucfile = new File(fileName);
			    if(sucfile.delete())
	    		{
	    			log.debug("文件:[" + fileName + " ]删除成功");
	    		}
			}
				catch (Exception e)
			    {
			    	logStr = "上传备份目录异常.";
			    	this.log.error(logStr, e);
			    }
			    finally
			    {
			    	ftp.disconnect();
			    }

	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}
