package com.turk.rpc;

import net.sf.json.JSONObject;

public class BaseMsg extends AbstractMsg{

	public BaseMsg getByJson(String json)
	{
		JSONObject jsonobject = JSONObject.fromObject(json);
		BaseMsg register = null;
		register = (BaseMsg)JSONObject.toBean(jsonobject,
				BaseMsg.class);
		
		return register;
	}
}
