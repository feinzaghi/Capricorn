package com.turk.socket;
import java.net.*;
import java.io.*;

import org.apache.log4j.Logger;

import com.turk.util.LogMgr;


/**
 * Socket �ͻ���
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
	 * �����˷�����Ϣ
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
				//client.setSoTimeout(60*1000);//60�볬ʱ
				out.println(msg);
				out.println("bye"); //��Ϊ����˽��ս������ж�
				out.flush();
				String result = buf.readLine();
				return result;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				log.error("Socket �ͻ����쳣,�ȴ�1s ��������",e);
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
		log.warn("Socket �ͻ����쳣,����3�κ�ʧ��");
		return "CLOSE";
	}
}
