package com.turk.distributor;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;

import com.turk.config.SystemConfig;
import com.turk.exception.ParseException;
import com.turk.exception.StoreException;
import com.turk.store.SqlldrStore;
import com.turk.store.SqlldrStoreParam;
import com.turk.task.CollectObjInfo;
import com.turk.templet.GenericSectionHeadD;
import com.turk.templet.GenericSectionHeadP;
import com.turk.templet.Table;
import com.turk.util.Util;

public class GenericSectionHeadDistributor extends Distribute
{
	protected SqlldrStore sqlldrStore;
	private List<File> clobFiles = new ArrayList<File>();
	private int clobIndex;

	public GenericSectionHeadDistributor()
	{
	}

	public GenericSectionHeadDistributor(CollectObjInfo TaskInfo)
	{
		super(TaskInfo);
	}

	public void init(CollectObjInfo TaskInfo)
	{
		super.init(TaskInfo);
	}

	protected void init()
	{
	}

	public void distribute(Object wParam, Object lParam)
    	throws Exception
    {
		GenericSectionHeadP.Templet templetP = (GenericSectionHeadP.Templet)wParam;
		int dsID = ((Integer)lParam).intValue();

		if ((!(this.disTmp instanceof GenericSectionHeadD)) || (dsID < 0)) {
			return;
		}
		int pTempletID = templetP.getId();
		GenericSectionHeadP.Public pubP = templetP.getPublicElement();
		GenericSectionHeadP.DS dsP = (GenericSectionHeadP.DS)templetP.getDsMap().get(Integer.valueOf(dsID));
		GenericSectionHeadD.Templet templetD = ((GenericSectionHeadD)this.disTmp).getTemplet(pTempletID);
		Table tableD = (Table)templetD.getTables().get(Integer.valueOf(dsID));
		if (tableD == null) {
			throw new ParseException("在分发模板中找不到对应的数据区域编号(id=" + dsID + ")");
		}

		if (this.sqlldrStore == null)
		{
			this.sqlldrStore = new SqlldrStore(new SqlldrStoreParam(pTempletID, tableD));
			this.sqlldrStore.setCollectInfo(this.collectInfo);
			this.sqlldrStore.setTaskID(this.collectInfo.getTaskID());
			this.sqlldrStore.setDataTime(this.collectInfo.getLastCollectTime());
			this.sqlldrStore.setDeviceID(this.collectInfo.getDevInfo().getDevID());
			this.sqlldrStore.setFlag(Util.getDateString_yyyyMMddHHmmssSSS(new Date()) + 
					"_" + Math.abs(new Random().nextInt()));
			this.sqlldrStore.open();
		}

		String splitSignD = tableD.getSplitSign();
		StringBuilder dataRow = new StringBuilder();

		if (pubP != null)
		{
			Collection<GenericSectionHeadP.Field> pubFileds = pubP.getFields().getFields().values();
			for (GenericSectionHeadP.Field f : pubFileds)
			{
				dataRow.append(nvl(f.getValue())).append(splitSignD);
			}

		}

		Collection<GenericSectionHeadP.Field> dsFileds = dsP.getFields().getFields().values();
		for (GenericSectionHeadP.Field f : dsFileds)
		{
			if (((Table.Column)tableD.getColumns().get(Integer.valueOf(f.getIndex()))).getType() == 4)
			{
				File clob = new File(SystemConfig.getInstance().getCurrentPath(), "clob_" + 
						this.collectInfo.getTaskID() + 
						"_" + 
						Util.getDateString_yyyyMMddHHmmssSSS(this.collectInfo.getLastCollectTime()) + 
						"_" + this.clobIndex++ + ".clob");
				PrintWriter pw = new PrintWriter(clob);
				pw.print(f.getValue() == null ? "" : f.getValue());
				pw.flush();
				pw.close();
				dataRow.append(clob.getAbsolutePath()).append(splitSignD);
				this.clobFiles.add(clob);
			}
			else
			{
				dataRow.append(nvl(f.getValue())).append(splitSignD);
			}
		}
		this.sqlldrStore.write(dataRow.toString());
    }

	private String nvl(String value)
	{
		return value == null ? "" : value;
	}

	public void commit()
	{
		if (this.sqlldrStore != null)
		{
			try
			{
				this.sqlldrStore.flush();
				this.sqlldrStore.commit();
				this.sqlldrStore.close();
				this.sqlldrStore = null;
				for (File f : this.clobFiles)
				{
					f.delete();
				}
				this.clobFiles.clear();
			}
			catch (StoreException e)
			{
				e.printStackTrace();
			}
		}
	}
}