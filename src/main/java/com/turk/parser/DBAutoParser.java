package com.turk.parser;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Map;

import com.turk.exception.StoreException;
import com.turk.store.SqlldrStore;
import com.turk.store.SqlldrStoreParam;
import com.turk.templet.DBAutoTempletP;
import com.turk.templet.GenericSectionHeadD;
import com.turk.templet.Table;

/**
 * 数据库自动解析，不需要配置解析模版，全字段匹配解析
 * @author Administrator
 *
 */
public class DBAutoParser extends Parser
{
	private SqlldrStore sqlldrStore;

	public boolean parseData()
    	throws Exception
    {
		throw new UnsupportedOperationException();
    }

	public int parseData(ResultSet rs, DBAutoTempletP.Templet parseTemp) throws Exception
	{
	    Table tableD = null;
	    int recordCount = 0;
	    int tempId = parseTemp.getId();
	    GenericSectionHeadD.Templet templetD = ((GenericSectionHeadD)this.collectObjInfo.getDistributeTemplet()).getTemplet(tempId);
	    if (templetD == null)
	      throw new Exception("在分发模板中找不到对应的编号(解析编号=" + tempId + ")");
	    tableD = (Table)templetD.getTables().get(Integer.valueOf(0));
	    String splitSign = tableD.getSplitSign();
	    ResultSetMetaData meta = rs.getMetaData();
	    Map<Integer,DBAutoTempletP.Field> fields = parseTemp.getFields();
	    parseMeta(meta, fields);
	    while (rs.next())
	    {
		      StringBuilder colVals = new StringBuilder();
		      for (DBAutoTempletP.Field f : fields.values())
		      {
			        int index = f.getIndexInHead();
			
			        if (index > -1)
			        {
			          String colVal = rs.getString(index);
			          colVals.append(removeNoise(f.getColType(), colVal)).append(splitSign);
			        }
			        else
			        {
			          colVals.append("").append(splitSign);
			        }
		      }
		      recordCount++;
		      distribute(colVals.toString(), tempId, tableD);
	    }
	    commit();
	    return recordCount;
	}

	private void parseMeta(ResultSetMetaData meta, Map<Integer, DBAutoTempletP.Field> fields)
    	throws Exception
    {
		int colCount = meta.getColumnCount();
		for (DBAutoTempletP.Field f : fields.values())
		{
			for (int i = 1; i <= colCount; i++)
			{
				int colType = meta.getColumnType(i);
				String colName = meta.getColumnName(i);
				if (!colName.equalsIgnoreCase(f.getName()))
					continue;
				f.setIndexInHead(i);
				f.setColType(colType);
				break;
			}

			if ((f.getOccur() == null) || 
					(!f.getOccur().equalsIgnoreCase("required")) || 
					(f.getIndexInHead() != -1)) continue;
			throw new Exception("required字段(" + f.getName() + ")不存在,放弃解析");
		}
    }

	private void distribute(String lineData, int templetId, Table tableD)
		throws StoreException
    {
		if (this.sqlldrStore == null)
		{
			this.sqlldrStore = new SqlldrStore(new SqlldrStoreParam(templetId, tableD));
			this.sqlldrStore.setCollectInfo(this.collectObjInfo);
			this.sqlldrStore.setTaskID(this.collectObjInfo.getTaskID());
			this.sqlldrStore.setDataTime(this.collectObjInfo.getLastCollectTime());
			this.sqlldrStore.setDeviceID(this.collectObjInfo.getDevInfo().getDevID());
			this.sqlldrStore.open();
		}
		this.sqlldrStore.write(lineData);
    }

	private void commit()
	{
		if (this.sqlldrStore != null)
		{
			try
			{
				this.sqlldrStore.flush();
				this.sqlldrStore.commit();
				this.sqlldrStore.close();
    		    this.sqlldrStore = null;
			}
			catch (StoreException localStoreException)
			{
			}
		}
 	}

	private String removeNoise(int colType, String colVal)
	{
		if (colVal == null) {
			return "";
		}

		if ((colType == 91) || (colType == 92) || (colType == 93)) {
			return colVal.substring(0, 19);
		}

		colVal = colVal.trim().replaceAll(";", " ").replaceAll("\r\n", " ");
		colVal = colVal.replaceAll("\n", " ").replaceAll("\r", " ");
    
		return colVal;
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}
}