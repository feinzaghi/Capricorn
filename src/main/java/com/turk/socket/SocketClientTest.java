package com.turk.socket;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.log4j.Logger;

import com.turk.parser.cdr.hw.CommonFunc;
import com.turk.parser.taurus.MonitorHit;

import com.turk.util.LogMgr;

public class SocketClientTest {

private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private int _port=8000;
	private String _ip = "";
	public SocketClientTest(String IP,int Port){
		
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
		OutputStream obyte = null;
		/*Socket s=new Socket(host,port);
		OutputStream out = s.getOutputStream();
		out.write(data);
		out.close();
		s.close();*/
		try {
			client=new Socket(_ip,_port);
			log.debug("Client connect server success!");
			obyte = client.getOutputStream();
			byte[] data1 = new byte[2];
			int MobileEventType = 21;
			int nOrderID = 0;
			data1 = CommonFunc.int2byte(MobileEventType);
			obyte.write(data1);
			byte[] data2 = new byte[8];
			obyte.write(data1);
			
			BufferedReader buf=new BufferedReader(new InputStreamReader(client.getInputStream()));
			out = new PrintWriter(client.getOutputStream());
			
			FileInputStream fis = null;
	  		try
	  		{
	  			

	  			File fs = new File("D:\\Workspaces\\MyEclipse\\Capricorn\\data\\990000103\\File\\a1_cc_bdr\\a1_cc_201304221115_00.AVL");
	      
	  			fis = new FileInputStream(fs);
	      
	  			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
	  			String line = br.readLine();
	  			while(line != null)
	  			{
	  				out.println(line);
	  				line = br.readLine();
	  			}
	  		
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
	  		
		
			out.println("bye"); //作为服务端接收结束的判断
			//out.flush();
			//String result = buf.readLine();
			return "";
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("Socket 客户端异常",e);
			return "Failure";
		} finally {
			try {
				if(out!=null)
				{
					out.close();
					obyte.close();
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
	
	public static void main(String[] args)
	{
		SocketClientTest c = new SocketClientTest("127.0.0.1",9001);
		c.SendMsg("");
	}
	
}
