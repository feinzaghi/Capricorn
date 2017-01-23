package com.turk.framework;

import org.apache.log4j.Logger;

import com.turk.access.AbstractAccessor;
import com.turk.bean.PBeanMgr;
import com.turk.clusters.slave.IExecute;
import com.turk.distributor.Distribute;
import com.turk.parser.Parser;
import com.turk.task.CollectObjInfo;
import com.turk.templet.AbstractTempletBase;
import com.turk.templet.TempletBase;
import com.turk.templet.TempletRecord;
import com.turk.util.LogMgr;

/**
 * 采集解析对象工厂
 * @author Administrator
 *
 */
public class Factory
{
	private static Logger logger = LogMgr.getInstance().getSystemLogger();

	public static AbstractAccessor createAccessor(CollectObjInfo obj)
	{
		if (obj == null) {
			return null;
		}
		AbstractAccessor accessor = PBeanMgr.getInstance().getAccessorBean(obj.getCollectType());
		if (accessor == null) {
			return null;
		}
		accessor.setTaskInfo(obj);

		Parser parser = createParser(obj);
		if (parser == null)
		{
			logger.error("未找到parserId为" + obj.getParserID() + 
				"的解析器，请查看pbean.xml是否有此parser");
		}

		Distribute distributor = createDistributor(obj);
		parser.setDistribute(distributor);

		accessor.setParser(parser);
		accessor.setDistributor(distributor);

		return accessor;
	}

	public static Parser createParser(CollectObjInfo obj)
	{
		if (obj == null) {
			return null;
		}
		int parserID = obj.getParserID();

		Parser p = PBeanMgr.getInstance().getParserBean(parserID);
		if (p == null) {
			return null;
		}

		p.setCollectObjInfo(obj);

		return p;
	}

	public static Distribute createDistributor(CollectObjInfo obj)
	{
		if (obj == null) {
			return null;
		}
		int distID = obj.getDistributorID();

		Distribute d = PBeanMgr.getInstance().getDistributorBean(distID);
		if (d == null)
			return null;
		d.init(obj);

		return d;
	}
	
	public static IExecute createSlaveExecute(int id)
	{
		
		IExecute d = PBeanMgr.getInstance().getSlaveBean(id);
		if (d == null)
			return null;
		return d;
	}

	public static TempletBase createTemplet(int tmpType, int tmpID)
	{
		TempletBase templet = PBeanMgr.getInstance().getTemplateBean(tmpType);
		if (templet == null) {
			return null;
		}

		templet.buildTmp(tmpID);

		return templet;
	}

	public static AbstractTempletBase createTemplet(TempletRecord record)
	{
		if (record == null) {
			return null;
		}
		AbstractTempletBase templet = PBeanMgr.getInstance().getTemplateBean(record.getType());
		if (templet == null) {
			return null;
		}
		
		templet.buildTmp(record);

		return templet;
	}
}