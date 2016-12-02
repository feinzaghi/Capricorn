package com.turk.access;

import java.util.ArrayList;
import java.util.List;

import com.turk.util.Util;

public class GenericDataConfig
{
	private String[] dataCfg;

	public String[] getDatas()
	{
		return this.dataCfg;
	}

	public void setDatas(String[] dataCfg)
	{
		this.dataCfg = dataCfg;
	}

	public static GenericDataConfig wrap(String strCfg)
	{
		GenericDataConfig cfg = null;

		if ((strCfg != null) && (strCfg.length() > 0))
		{
			String[] strFields = strCfg.split(";");
      
			List<String> tmp = new ArrayList<String>();
			for (String s : strFields)
			{
				if (!Util.isNotNull(s))
					continue;
				tmp.add(s.trim());
			}

			cfg = new GenericDataConfig();
			cfg.setDatas((String[])tmp.toArray(new String[0]));
		}
		return cfg;
	}
}