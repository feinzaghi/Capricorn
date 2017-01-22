package com.turk.clusters.slave;

import java.io.IOException;
import java.net.ServerSocket;

import org.apache.log4j.Logger;

//import com.turk.clusters.slave.Handler;
import com.turk.rpc.NettyServer;
import com.turk.util.LogMgr;

/**
 * �ڵ����̨
 * @author Administrator
 *
 */
public class SlaveConsole {

	private Logger log = LogMgr.getInstance().getAppLogger("slave");
	
	private int port = SlaveConfig.getInstance().getSlavePort();
	ServerSocket serverSocket;
	
	private Thread mainThread;
	
	public SlaveConsole()
	{
		try 
		{
			//����Socket����
			//serverSocket=new ServerSocket(port);
			log.debug("Start Slave ["+ port +"] Socket listener Success!");
		} catch (Exception e) {
			log.warn("Start Slave ["+ port +"] Socket listener Failure!",e);
		}
	}
	
	/*
	 * ��������
	 */
// 	public void start()
// 		throws IOException
//    {
// 		this.mainThread = new Thread(new Runnable()
// 		{
// 			public void run()
// 			{
// 				service("start");
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
// 	public void service(String msg)
//	{
//		while(true)
//		{
//			Socket socket=null;
//			try {
//				socket=serverSocket.accept();
//				Thread workThread=new Thread(new Handler(socket,msg));
//				workThread.start();
//			} catch (IOException e) {
//				// TODO �Զ����� catch ��
//				e.printStackTrace();
//			}
//		}
//	}
}

//class Handler implements Runnable{   //�����뵥���ͻ���ͨ��
//	
//	private Logger log = LogMgr.getInstance().getAppLogger("slave");
//	  private Socket socket;
//	  
//	  public Handler(Socket socket,String infomation){
//		  this.socket=socket;
//		  //this.infomation=infomation;
//	  }
//	  private PrintWriter getWriter(Socket socket)throws IOException{
//		  return new PrintWriter(socket.getOutputStream());
//	  }
//	  private BufferedReader getReader(Socket socket)throws IOException{
//		  return new BufferedReader(new InputStreamReader(socket.getInputStream()));
//	  }
//	  
//	  public void run()
//	  {
//		  PrintWriter pw = null;
//		  try 
//		  {
//		      log.debug("New connection accepted " +
//		      socket.getInetAddress() + ":" +socket.getPort());
//		      BufferedReader br = getReader(socket);
//		      pw = getWriter(socket);
//	     
//		      String line = br.readLine(); //�յ��ͻ�����Ϣ
//		      String msg = "";
//		      while(!line.equals("bye"))
//		      {
//		    	  msg = msg + line;
//		    	  line=br.readLine();
//		      }
//		      
//		      log.debug("�ͻ��������Ϣ��"+msg);
//		      
//		      //socket.close();
//		      
//		      if(!msg.isEmpty())
//		      {
//		    	  //����������Ϣ
//		    	  JSONObject jsonObject = JSONObject.fromObject(msg); 
//		    	  Object bean = JSONObject.toBean(jsonObject);
//		    	  int MsgID = 0;
//		    	  try 
//		    	  {
//		    		  assertEquals(jsonObject.get("msgID"),
//							  PropertyUtils.getProperty(bean, "msgID"));
//		    		  //��ȡ����Ϣ��
//		    		  MsgID = Integer.parseInt(PropertyUtils.getProperty(bean, "msgID").toString());
//		    		  
//				  } catch (Exception e) {
//						// TODO Auto-generated catch block
//						log.error("Master,�����ͻ���JSON MSGID�����쳣",e);
//				  }
//				  
//				  //�ڵ�����������б�
//				  switch(MsgID)
//				  {
//				      case 2001://�ɼ�������
//				    	  
//				    	  if(ThreadPool.getInstance().getThreadQueueCount()>100)
//				    	  {
//				    		  pw.println("NO");//���ؿͻ�����Ϣ
//						      pw.flush();
//						      return;
//				    	  }
//				    	  TaskMsg obj = new TaskMsg();
//				    	  obj = obj.getByJson(msg);
//				    	  if(obj != null)
//				    	  {
//					    	  TaskExecute exe = new TaskExecute();
//					    	  if(exe.Execute(obj))
//					    	  {
//					    		  log.debug("Task:[" + obj.getTaskID() +"] Start....");	
//					    		  TaskMsg task = new TaskMsg();
//						  		  task.setMsgID(2003);//֪ͨ���أ������Ѽ����б������ش������б���ɾ��������
//						  		  task.setTaskID(obj.getTaskID());
//						  		  task.setObjMap(obj.getObjMap());
//						  	  	  JSONObject json = JSONObject.fromObject(task);
//						  		    //System.out.println(jsonObject);
//						  		  log.debug("2003-MSG:�����Ѽ����б�" + json.toString());
//						  		  Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
//						  		    		SlaveConfig.getInstance().getMasterPort());
//						  		  String Result = clt.SendMsg(json.toString());
//						  		  if(Result.equals("Done"))
//						  		  {
//						  		      log.debug("2003-MSG:["+obj.getTaskID()+"] Complete!");
//						  		  }
//					    	  }
//					    	  else
//					    	  {
//					    		  log.warn("����ִ���쳣");
//					    	  }
//				    	  }
//				    	  else
//				    	  {
//				    		  log.warn("�������Ϊ null");
//				    	  }
//				    	  break;
//				      case 2003://����������
//				    	  StatTaskInfo objstat = new StatTaskInfo();
//				    	  objstat = objstat.getByJson(msg);
//				    	  if(objstat != null)
//				    	  {
//					    	  StatTaskExecute exe = new StatTaskExecute();
//					    	  exe.Execute(objstat);
//					    	  log.debug("StatTask:[" + objstat.getID() +"] Start....");
//				    	  }
//				    	  break;
//				      case 9999:
//				    	  shutdown();
//				    	  pw.println("Done");//���ؿͻ�����Ϣ
//					      pw.flush();
//				    	  log.debug("Close console service.");
//						  ConsoleMgr.getInstance(SystemConfig.getInstance().getCollectPort()).stop();
//						  log.debug("exit!");
//						  System.exit(0);
//				    	  return;
//				      default:
//				    	  break;	  
//				  }
//					
//		      }
//		      pw.println("Done");//���ؿͻ�����Ϣ
//		      pw.flush();
//		      
//		  }catch (IOException e) {
//			  e.printStackTrace();
//		  }finally {
//			  try{
//				  if(pw!=null)
//					  pw.close();
//				  if(socket!=null)
//					  socket.close(); //�Ͽ�����
//			  }catch (IOException e) 
//			  {
//				  e.printStackTrace();
//			  }
//		  }
//	  }
//	  
//	  private void shutdown()
//	  {
//		  log.debug("receive shutdown command");
//		  ThreadPool.getInstance().stoptask();//ֹͣ��ʵ��stop����������
//		  SlaveActive.getInstance().Stop();
//		  while(TaskMgr.getInstance().size() > 0)
//		  {
//			  try 
//			  {
//					Thread.sleep(10000L);
//					log.debug("waiting for stop");
//							
//			  } catch (InterruptedException e) {
//							// TODO Auto-generated catch block
//					e.printStackTrace();
//			  }
//		  }
//					
//					log.debug("No task running,close thread pool.");
//
//					ThreadPool.getInstance().destroy();
//
//					if(SystemConfig.getInstance().isEnableRunDTStatistic())
//					{
//						//����
//						try
//						{
//							appinterface appdt = ((appinterface)Class.forName("DT.DTMain").newInstance()).getInstance();
//							appdt.Shutdown();
//						}
//						catch (Exception e)
//						{
//							e.printStackTrace();
//						}
//					}
//					
//					ProcessStatus.getInstance().stopScan();
//					
//					DataLifecycleMgr.getInstance().stop();
//					AlarmMgr.getInstance().shutdown();
//					DataLogMgr.getInstance().commit();
//					LogMgr.getInstance().getDBLogger().dispose();
//					log.debug("Close DB Pool.");
//					CommonDB.closeDbConnection();
//					DbPool.close();
//					GPDBPool.close();
//	  }
//}
