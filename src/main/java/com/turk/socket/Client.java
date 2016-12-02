package com.turk.socket;
import java.net.*;
import java.io.*;

import org.apache.log4j.Logger;

import com.turk.util.LogMgr;


/**
 * Socket 客户端
 * @author Turk
 *
 */
public class Client {
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private int _port=8000;
	private String _ip = "";
	public Client(String IP,int Port){
		
		this._ip = IP;
		this._port = Port;
		
	}
	
	/**
	 * 向服务端发送消息
	 * @param msg
	 * @return
	 */
	public String SendMsg(String msg)
	{
		Socket client = null;
		PrintWriter out = null;
		int ntryCount = 0;
		while(ntryCount < 3)
		{
			try {
				
				client=new Socket(_ip,_port);
				log.debug("Client connect server success!");
				BufferedReader buf=new BufferedReader(new InputStreamReader(client.getInputStream()));
				out = new PrintWriter(client.getOutputStream());
				//client.setSoTimeout(60*1000);//60秒超时
				out.println(msg);
				out.println("bye"); //作为服务端接收结束的判断
				out.flush();
				String result = buf.readLine();
				return result;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.error("Socket 客户端异常,等待1s 重新连接",e);
				ntryCount++;
				try {
					Thread.sleep(1000L);
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				continue;
			} finally {
				try {
					if(out!=null)
					{
						out.close();
					}
					if(client!=null)
					{
						client.close();
					}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						log.error(e);
					}	
			}
		}
		log.warn("Socket 客户端异常,重试3次后失败");
		return "CLOSE";
	}
}
