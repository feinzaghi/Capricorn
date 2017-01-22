package com.turk.clusters.master;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

import net.sf.json.JSONObject;
import static org.junit.Assert.assertEquals;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;

import com.turk.rpc.NettyServer;
import com.turk.clusters.model.Register;
import com.turk.clusters.model.SlaveInfo;
import com.turk.clusters.model.TaskMsg;
import com.turk.util.LogMgr;

/**
 * ���ڵ����̨
 * @author Administrator
 *
 */
public class MasterConsole{
	
	private Logger log = LogMgr.getInstance().getAppLogger("master");
	
	private int port = 9527; //Ĭ�Ϸ���˼����˿�
	ServerSocket serverSocket;
	
	
	public static void main(String[] args)
	{
		try {
			MasterConsole master = new MasterConsole();
			master.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private Thread mainThread;
	
	public MasterConsole()
	{
		try 
		{
			//����Socket����
			//serverSocket=new ServerSocket(port);
			log.debug("Start Master Socket Port[" + port + "] listener Success!");
		} catch (Exception e) {
			log.warn("Start Master Socket Port[" + port + "] listener Failure!",e);
		}
	}
	
	/**
	 * ��������
	 */
// 	public void start()
// 		throws IOException
//    {
// 		this.mainThread = new Thread(new Runnable()
// 		{
// 			public void run()
// 			{
// 				service("master start");
// 			}
// 		});
// 		this.mainThread.start();
//    }
 	
 	public void start()
 	 		throws IOException
 	    {
 	 		this.mainThread = new Thread(new Runnable()
 	 		{
 	 			public void run()
 	 			{
// 	 				service("master start");
 	 				try {//Netty Server 2016/11/24 by turk
 						new NettyServer().bind(port);
 						
 					} catch (Exception e) {
 						// TODO Auto-generated catch block
 						log.error("Start Netty Sever Error",e);
 					}
 	 			}
 	 		});
 	 		this.mainThread.start();
 	 		log.info("Start Master Socket Port[" + port + "] listener Success!");
 	    }
 	
 	/**
 	 * ��������
 	 * @param msg
 	 */
 	public void service(String msg)
	{
		while(true)
		{
			Socket socket=null;
			try {
				
				socket=serverSocket.accept();
				Thread workThread=new Thread(new Handler(socket,msg));
				workThread.start();
				//Thread.sleep(1000L);
			} catch (Exception e) {
				// TODO �Զ����� catch ��
				//e.printStackTrace();
				log.error("Master console service error",e);
			} finally
			{
				
			}
		}
	}
}

class Handler implements Runnable{   //�����뵥���ͻ���ͨ��
	
	private Logger log = LogMgr.getInstance().getAppLogger("master");
	private Socket socket;
	private String infomation;
	  
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
			
			
			String line = br.readLine(); //�յ��ͻ�����Ϣ
			String msg = "";
			while(line!=null&&!line.equals("bye"))
			{
				msg = msg + line;
				line=br.readLine();
			}
		      
			log.debug("�ͻ��������Ϣ��"+msg);
			pw.println("Done");//���ؿͻ�����Ϣ
			pw.flush();
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
					log.error("Master,�����ͻ���JSON MSGID�����쳣",e);
				}
				
				//����������Ϣ�б�
				switch(MsgID)
				{
					case 1001://ע����Ϣ
						Register reg1 = new Register();
						TaskManage.getInstance().NodeRegister(reg1.getByJson(msg));
						break;
					case 1002://����ͬ����Ϣ
						Register reg2 = new Register();
						TaskManage.getInstance().UpdateSlaveStatus(reg2.getByJson(msg));
						break;
					case 1003://�ڵ�֪ͨ�ر�
						Register reg3 = new Register();
						TaskManage.getInstance().UpdateSlaveStatus(reg3.getByJson(msg));
						break;
					case 2002://�������
						TaskMsg task = new TaskMsg();
						task = task.getByJson(msg);
						String server2 = socket.getInetAddress().getHostAddress();
						SlaveInfo slave2 = TaskManage.getInstance().getSlaves(server2);
						if(slave2 != null)
						{
							String key = task.getTaskID() + "#" + task.getObjMap().get("suc_data_time");
							TaskMsg taskMsg = slave2.deleteTask(key);
							if(taskMsg!=null)
								log.debug("Task:2002:["+key+"]["+ slave2.getServer() +"] Success!");
							else
								log.warn("Task Obj ["+key+"] is not exist!");
							slave2.setCurrentCltCount(slave2.getCurrentCltCount() - 1);
						}
						break;	
					case 2003://֪ͨ�����ͳɹ�
						TaskMsg taskruning = new TaskMsg();
						taskruning = taskruning.getByJson(msg);
						TaskManage.getInstance().delActiveTask(taskruning.getTaskID(), false);
						String server3 = socket.getInetAddress().getHostAddress();
						SlaveInfo slave3 = TaskManage.getInstance().getSlaves(server3);
						if(slave3 != null)
						{
							String key = taskruning.getTaskID() + "#" + taskruning.getObjMap().get("suc_data_time");
							slave3.setTaskMaps(key, taskruning);
							slave3.setCurrentCltCount(slave3.getCurrentCltCount() + 1);
							log.debug("Task:2003:["+key+"]["+ slave3.getServer() +"] Success!");
						}
						break;	
					default:
						break;	  
				 }
					
		     }
		      
		}catch (IOException e) {
			  log.error("Master,������Ϣ�쳣",e);
			  pw.println(e.getMessage());//���ؿͻ�����Ϣ
		}finally {
			try{
				  if(pw!=null)
					  pw.close();
				  if(socket!=null)
					  socket.close(); //�Ͽ�����
				  
			  }catch (IOException e) 
			  {
				  log.error("Master,close socket �쳣",e);
			  }
		  }
	  }
}
