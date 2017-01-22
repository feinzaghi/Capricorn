package com.turk.rpc;

public class MessageToString implements IMessageCallBack{

	private String _returnString = "";
	
	public MessageToString(String ip,int port,String msg)
	{
		try {
//			BaseMsg basemsg = new BaseMsg();
//			basemsg.setMsgID(MsgID);
//			JSONObject json = JSONObject.fromObject(basemsg);
			new NettyClient(this).connect(port, ip, msg);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void ReturnMessage(String msg) {
		// TODO Auto-generated method stub
		_returnString = msg;
	}
	
	public String getReturnString()
	{
		return _returnString;
	}
}
