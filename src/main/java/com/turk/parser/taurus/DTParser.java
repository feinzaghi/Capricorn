package com.turk.parser.taurus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.turk.parser.Parser;
import com.turk.parser.taurus.model.CFG_MAP_PN_CELL;

import com.turk.templet.LineTempletP;
import com.turk.util.GISUtil;
import com.turk.util.Util;

public class DTParser extends Parser{
	//private Logger logger = LogMgr.getInstance().getSystemLogger();
	String remainingData = "";
	private GISUtil _gis;
	
	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		this.log.debug(this.collectObjInfo.getTaskID() + 
				":Start parser taurus DT file：" + this.fileName);

		_gis = new GISUtil(this.collectObjInfo.getDevInfo().getCityID());
		
	    
  		FileInputStream fis = null;
  		try
  		{
  			String logStr = this + ": starting parse file : " + this.fileName;
  			this.log.debug(logStr);
  			this.collectObjInfo.log("解析", logStr);

  			File fs = new File(this.fileName);
      
  			if (!fs.exists())
  		    {
  		    	this.log.error(this.collectObjInfo.getTaskID() + ":File does not exist：" + this.fileName);
  		    	return false;
  		    }
  			new MonitorHit(fs.getName());
  			
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
  			for (int i = 0; i < nRowCount; i++)
  			{
  				if (Util.isNull(strzRowData[i]))
  					continue;
  				ParseLineData(strzRowData[i]);
  			}
  		}
  		catch (Exception e)
  		{
  			bReturn = false;
  			logStr = this + ": Cause:";
  			this.log.error(logStr, e);
  			this.collectObjInfo.log("解析", logStr, e);
  		}

  		return bReturn;
  	}
	
	/**
  	 * 行解析
  	 * @param strOldRow
  	 */
	public void ParseLineData(String strOldRow)
	{
		int nSubTmpIndex = 0;

		//行解析模版
		LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();

		LineTempletP.SubTemplet subTemp = (LineTempletP.SubTemplet)templet.m_nTemplet
			.get(nSubTmpIndex);

		StringBuffer strNewRow = new StringBuffer();
		String strValue = "";
		//固定日期
		Date testdate = new Date(this.collectObjInfo.getLastCollectTime().getTime());
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
		strNewRow.append(formatter.format(testdate) + subTemp.m_strNewFieldSplitSign);
		
		try
		{
			strValue = ParseRowBySplit(subTemp, strOldRow);
			if(strValue.isEmpty())
				return;
		}
		catch (Exception e)
		{
			String str = this + " : error when parsing data. templet name : " + 
				templet.tmpName + " data:" + strOldRow;
			this.log.error(str, e);
			this.collectObjInfo.log("解析", str, e);
			return;
		}
		strNewRow.append(strValue);
		strNewRow.deleteCharAt(strNewRow.length()-1);//去掉最后一个逗号
		strNewRow.append("\n");
		try {
			this.distribute.DistributeData(strNewRow.toString().getBytes("UTF-8"), nSubTmpIndex);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			log.error("转换UTF-8格式异常",e);
		}
	}
	
	
	
	private String ParseRowBySplit(LineTempletP.SubTemplet subTemp, String strRow)
	{
		StringBuffer m_TempString = new StringBuffer();
		LineTempletP templet = (LineTempletP)this.collectObjInfo.getParseTemplet();
		
		String[] m_strTemp;
		if ((subTemp.m_strFieldUpSplitSign == null) || 
				(subTemp.m_strFieldUpSplitSign.length() == 0))
			m_strTemp = strRow.split(subTemp.m_strFieldSplitSign);
		else {
			m_strTemp = split(strRow, subTemp.m_strFieldSplitSign, subTemp.m_strFieldUpSplitSign);
		}
		int nCount = 0;
		Map<String,String> kv = new HashMap<String, String>();
		String nvl = subTemp.nvl;
		
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
					//将行值加入键值对
					kv.put(subTemp.m_Filed.get(k).m_strFieldName, removeNoiseSemicolon(m_strTemp[k].trim()));
				}
				catch (Exception ex)
				{
					log.error("将解析数据加入键值对时报错",ex);
					//kv.put(subTemp.m_Filed.get(k).m_strFieldName, removeNoiseSemicolon(m_strTemp[k].trim()));
				}
			}
			nCount++;
		}
		
		String str = Util.findByRegex(kv.get("Longitude"), "^\\d+(\\.\\d+)?$", 0);
		if(str == null)
			return "";
		
		//grid_id	integer
		double lon = Double.parseDouble(kv.get("Longitude"));
		double lat = Double.parseDouble(kv.get("Latitude"));
		long GRIDID = 0;
		if(lon != -10000 && lat != -10000)
			GRIDID = _gis.GetGridID(100, lon, lat);
		m_TempString.append(GRIDID + subTemp.m_strNewFieldSplitSign);
		//Longitude	double precision
		m_TempString.append(kv.get("Longitude") + subTemp.m_strNewFieldSplitSign);
		//Latitude	double precision
		m_TempString.append(kv.get("Latitude") + subTemp.m_strNewFieldSplitSign);
		
		//pn	smallint
		int pn = 0;
		if(kv.get("ReferencePN")!=null)
			pn = (int)Double.parseDouble(kv.get("ReferencePN"));
		m_TempString.append(pn + subTemp.m_strNewFieldSplitSign);
		
		//cell_sys_id	bigint
		
		ArrayList<CFG_MAP_PN_CELL> celllist = MapPNCell.getInstance().getCellList(pn);
		double shortdis = 1000000;
		CFG_MAP_PN_CELL nearCell = null; 
		if(celllist!=null)
		{
			for(CFG_MAP_PN_CELL cell : celllist)
			{
				double distance = GISUtil.GetDistance(lon, lat, cell.getLongitude(), cell.getLatitude());
				if(distance < shortdis)
				{
					shortdis = distance;
					nearCell = cell;
				}
			}
		}
		
		if(nearCell != null)
		{
			m_TempString.append(nearCell.getCellSysID() + subTemp.m_strNewFieldSplitSign);
		}
		else if(kv.get("LAC")!=null && kv.get("CI")!=null)
		{
			long lac = Long.parseLong(kv.get("LAC"));
			long ci = Long.parseLong(kv.get("CI"));
			
			long cellsysid = 5*10000000000000L + 531*10000000000L +lac*100000+ci;

			m_TempString.append(cellsysid + subTemp.m_strNewFieldSplitSign);
		}
		else
		{
			m_TempString.append("" + subTemp.m_strNewFieldSplitSign);
		}
		
		m_TempString.append("531" + subTemp.m_strNewFieldSplitSign);
		
		m_TempString.append("5" + subTemp.m_strNewFieldSplitSign);
		
		return m_TempString.toString();
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
	
}
