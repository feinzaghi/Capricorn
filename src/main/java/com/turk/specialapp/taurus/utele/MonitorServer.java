package com.turk.specialapp.taurus.utele;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;

import com.turk.util.LogMgr;

/**
 * ��ǰ̨�ӿڵļ������񣬽���ǰ̨����������������Ϣ
 * @author Administrator
 *
 */
public class MonitorServer {

	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private int port = 9901; //Ĭ�Ϸ���˼����˿�
	ServerSocket serverSocket;
	private boolean mStopFlag = true;
	
	private static MonitorServer _instance = null;
	
	public static MonitorServer getInstance()
	{
		if(_instance == null)
			_instance = new MonitorServer();
		return _instance;
	}
	
	public static void main(String[] args)
	{
		try {
			
			FlexAuthServer.getInstance().startServer();
			MonitorServer.getInstance().start();
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private Thread mainThread;
	
	private Thread checkThread;
	
	public MonitorServer()
	{
		try 
		{
			//����Socket����
			serverSocket=new ServerSocket(port);
			log.debug("Start Master Socket Port[" + port + "] listener Success!");
		} catch (IOException e) {
			log.warn("Start Master Socket Port[" + port + "] listener Failure!",e);
		}
	}
	
	/**
	 * ��������
	 */
 	public void start()
 		throws IOException
    {
 		this.mStopFlag = false;
 		this.mainThread = new Thread(new Runnable()
 		{
 			public void run()
 			{//��ָ���˿��������������տͻ��˷�����������Ϣ
 				service("Taurus listener[" + port + "] start");
 			}
 		});
 		this.mainThread.start();
 		
 		this.checkThread = new Thread(new Runnable()
 		{
 			public void run()
 			{//��ָ���˿��������������տͻ��˷�����������Ϣ
 				checkqueue();
 			}
 		});
 		this.checkThread.start();
    }
 	
 	/**
 	 * ��������
 	 * @param msg
 	 */
 	public void service(String msg)
	{
		while(!mStopFlag)
		{
			Socket socket=null;
			try {
				socket=serverSocket.accept();
				Thread workThread=new Thread(new Handler(socket,msg));
				workThread.start();
			} catch (IOException e) {
				// TODO �Զ����� catch ��
				e.printStackTrace();
			}
		}
		
		
	}
 	
 	public void checkqueue()
 	{
 		while(!mStopFlag)
		{
 			List<Integer> removeList = new ArrayList<Integer>();
 			
 		    for(int queuenum : MessageQueue.getInstance().GetAllQueue().keySet())
 		    {
 		    	RequestMsgInternal obj = MessageQueue.getInstance().GetAllQueue().get(queuenum);
 		    	RelayMsg relay = new RelayMsg();
				relay.setQueueNum(queuenum);
				relay.setMsgID(45004);
				relay.setStatus("CHECK");
				
				JSONObject jsonObject = JSONObject.fromObject(relay);
				
				
				log.debug("45004-MSG,Send��" + jsonObject.toString() + " QueueID:" + queuenum);

				int sendcount = 0;
				while(sendcount < 3 && !obj.getSocket().isClosed())
				{
					try
					{
						
						String strSend = jsonObject.toString() + "\n";
						OutputStream os = getOutputStream(obj.getSocket());
						os.write(strSend.getBytes("UTF-8"));
						os.flush();
						//������Ϣʧ�ܺ�ɾ����Ϣ����
											
						break;
					}
					catch(Exception e)
					{
						String strerr = String.format("send message error.server[%s] port[%d]",
								obj.getSocket().getLocalAddress().getHostAddress(),
								obj.getSocket().getPort());
						log.error(strerr,e);
						try {
							Thread.sleep(2000);
						} catch (InterruptedException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						sendcount++;
						continue;
					}
				}
				if(sendcount>=3)
				{
					log.debug("send message try 3 times,close socket connect!");
					//MessageQueue.getInstance().Remove(queuenum);
					removeList.add(queuenum);
					continue;
				}
 		    }
 		    
 		    for(Integer delqueue : removeList)
			{
				RequestMsgInternal request = MessageQueue.getInstance().GetAllQueue().get(delqueue);
				MessageQueue.getInstance().Remove(delqueue);
				log.debug("45004-MSG-["+request.getServer()+"] send failure,close connect!");
			}
 			
 			try {//5���Ӽ��һ��
				Thread.sleep(5*60*1000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
 	}
 	
 	 private OutputStream getOutputStream(Socket socket)throws IOException{
			return socket.getOutputStream();
		}
 	 
 	public void Stop()
 	{
 		this.mStopFlag = true;
 		try {
			if(serverSocket!=null)
				serverSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
 	}
}

class Handler implements Runnable{   //�����뵥���ͻ���ͨ��
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	private Socket socket;
	private String infomation;
	
	String xml = "<?xml version=\"1.0\"?><cross-domain-policy><site-control permitted-cross-domain-policies=\"all\"/><allow-access-from domain=\"*\" to-ports=\"*\"/></cross-domain-policy>\0";
	  
	public Handler(Socket socket,String infomation){
		this.socket=socket;
		this.infomation=infomation;
	}
	private PrintWriter getWriter(Socket socket)throws IOException{
		return new PrintWriter(socket.getOutputStream());
	}
	private BufferedReader getReader(Socket socket)throws IOException{
		return new BufferedReader(new InputStreamReader(socket.getInputStream()));
	}
	  
	public void run()
	{
		PrintWriter pw = null;
		try 
		{
			log.debug(this.infomation + "--New connection accepted " +
					socket.getInetAddress() + ":" +socket.getPort());
			BufferedReader br = getReader(socket);
			pw = getWriter(socket);
			
			//
			
			char[] by = new char[22];
			br.read(by, 0, 22);
			String s = new String(by);
			System.out.println("s="+s);
			if (s.equals("<policy-file-request/>")) {
				System.out.println("����policy-file-request");
				pw.print(xml);
				pw.flush();
				//br.close();
				//pw.close();
				//socket.close();
				return;
			} 
			 
			String line = s + br.readLine(); //�յ��ͻ�����Ϣ
			
			String msg = "";
			while(!line.equals("bye"))
			{
				msg = msg + line;
				line=br.readLine();
			}
		      
			log.debug("�ͻ��������Ϣ��"+msg);
			
		      //socket.close();
		      
			if(!msg.isEmpty())
			{
				//����������Ϣ
				JSONObject jsonObject = JSONObject.fromObject(msg); 
				Object bean = JSONObject.toBean(jsonObject);
				int MsgID = 0;
				try 
				{
					assertEquals(jsonObject.get("msgID"),
							PropertyUtils.getProperty(bean, "msgID"));
					//��ȡ����Ϣ��
					MsgID = Integer.parseInt(PropertyUtils.getProperty(bean, "msgID").toString());
		    		  
				} catch (Exception e) {
					// TODO Auto-generated catch block
					log.error("Taurus,�����ͻ���JSON MSGID�����쳣",e);
				}
				
				//����������Ϣ�б�
				switch(MsgID)
				{
					case 45001://�ͻ���������Ϣ
						//�õ��ͻ��˵����󣬼�¼������Ϣ�������ض��е���
						RequestMsg reg1 = new RequestMsg();
						reg1 = reg1.getByJson(msg);
						
						RequestMsgInternal msg2 = new RequestMsgInternal();
						msg2.setMsgID(reg1.getMsgID());
						msg2.setServer(reg1.getServer());
						msg2.setPort(reg1.getPort());
						msg2.setKey(reg1.getKey());
						msg2.setValue(reg1.getValue());
						msg2.setSocket(socket);
						
						int nQueuenum = MessageQueue.getInstance().Add(msg2);
						
						RelayMsg relay = new RelayMsg();
						relay.setQueueNum(nQueuenum);
						relay.setMsgID(45001);
						relay.setStatus("Y");
						
						jsonObject = JSONObject.fromObject(relay);
						pw.print(jsonObject.toString());
						pw.flush();
						log.debug("relay:"+jsonObject.toString());
						
						try {
							Thread.sleep(5000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						/*
						SendResult send = new SendResult();
						String data = "CC_TELECOM:2013-10-17 15:34:47.768681049,0,15306337583,15963828798,460030946336798,EF311FDA,196811,ɽ��,����,633,3,,ɽ��,����,633,ɽ���ƶ�GSM��,,633,,134678390,102584362,0,0,0,0,0,18,0,0,0,0,0,5,255,0,1094,1";
						List<Integer> queueList = new ArrayList();
						queueList.add(nQueuenum);
						send.SendData(queueList, data);*/
						
						//ʵʱ�û�����
						/*String msisdn = "13542458796";
						HashMap<String,List<Integer>> imsiMap = MessageQueue.getInstance().GetImsiQueue();
						if(imsiMap.containsKey(strimsi))
						{
							String data = "CC_TELECOM:" + strLine 
									+ subTemp.m_strNewFieldSplitSign + cell.getLon()
									+ subTemp.m_strNewFieldSplitSign + cell.getLat();
							SendResult send = new SendResult();
							send.SendData(imsiMap.get(strimsi), data);
						}
						
						HashMap<String,List<Integer>> msisdnMap = MessageQueue.getInstance().GetMsisdnQueue();
						if(msisdnMap.containsKey(msisdn))
						{
							String data = "CC_TELECOM:";
							SendResult send = new SendResult();
							send.SendData(msisdnMap.get(msisdn), data);
						}*/
						
						break;
					case 45003://�ͻ���������Ϣ
						CloseMsg msg1 = new CloseMsg();
						msg1 = msg1.getByJson(msg);
						
						MessageQueue.getInstance().Remove(msg1.getQueueNum());
						
						RelayMsg relay2 = new RelayMsg();
						relay2.setQueueNum(msg1.getQueueNum());
						relay2.setMsgID(45003);
						relay2.setStatus("Y");
						
						jsonObject = JSONObject.fromObject(relay2);
						pw.print(jsonObject.toString());
						pw.flush();
						log.debug("relay:"+jsonObject.toString());
						
						break;
					default:
						break;	  
				 }
					
		     }
		      
		}catch (IOException e) {
			  log.error("Taurus listener,������Ϣ�쳣",e);
			  pw.println(e.getMessage());//���ؿͻ�����Ϣ
		}finally {
			try{
				  //if(pw!=null)
					//  pw.close();
				  //if(socket!=null)
					//  socket.close(); //�Ͽ�����
				  
			  }catch (Exception e) 
			  {
				  log.error("Master,close socket �쳣",e);
			  }
		  }
	  }
}
