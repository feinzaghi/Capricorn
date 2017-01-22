package com.turk.rpc;

import java.util.HashSet;
import net.sf.json.JSONObject;

public class MessageToList implements IMessageCallBack{
	
	HashSet<String> _Result = new  HashSet<String>();
	
	private int _MsgID = 0;
	
	public MessageToList(String ip,int port,int MsgID)
	{
		try {
			BaseMsg basemsg = new BaseMsg();
			basemsg.setMsgID(MsgID);
			JSONObject json = JSONObject.fromObject(basemsg);
			new NettyClient(this).connect(port, ip, json.toString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void ReturnMessage(String msg) {
//		return msg;
		System.out.println(msg);
		String[] IPs = msg.split("\\|");
		HashSet<String> list = new HashSet<String> ();
		for(String ip:IPs)
		{
			list.add(ip);
		}
		
		_Result = list;
//		switch(_MsgID)
//		{
//			case 1002:
//				BasicMessage.getInstance().setIPFilterForLog(list);
//				break;
//			case 1004:
//				BasicMessage.getInstance().setIPWhiteList(list);
//				break;
//		}
		
	}
	
	public HashSet<String> getListMessage()
	{
		return _Result;
	}
}
