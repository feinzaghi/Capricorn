package com.turk.specialapp.taurus.utele;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.log4j.Logger;

import com.turk.util.LogMgr;

/**
 * 得到满足条件的结果返回给客户端
 * @author Administrator
 *
 */
public class SendResult{
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private Thread thread = null;
	
	/**
	 * 发送结果数据到客户端
	 * @param rstData
	 */
	public void SendData(final List<Integer> queueList,final String data) {
		// TODO Auto-generated method stub
		this.thread = new Thread(new Runnable()
 		{
 			public void run()
 			{
 				SendThread(queueList,data);
 			}
 		});
		
		this.thread.start();
	}
	
	/**
	 * 
	 * @param queueList
	 * @param data
	 */
	private void SendThread(List<Integer> queueList,String data)
	{
		try
		{
			HashMap<Integer,RequestMsgInternal> requestMap = MessageQueue.getInstance().GetAllQueue();
			
			List<Integer> removeList = new ArrayList<Integer>();
			
			for(Integer queuenum : queueList)
			{
				RequestMsgInternal request = requestMap.get(queuenum);
				if(request!=null)
				{
					ResultData rstData = new ResultData();
					
					rstData.setMsgID(45002);
					rstData.setData(data);
					JSONObject jsonObject = JSONObject.fromObject(rstData);

					log.debug("45002-MSG,Send：" + jsonObject.toString() + " QueueID:" + queuenum);

					int sendcount = 0;
					while(sendcount < 3 && !request.getSocket().isClosed())
					{
						try
						{
							//PrintWriter pw = getWriter(request.getSocket());
							
							//pw.write(jsonObject.toString());
							//pw.flush();
							String strSend = jsonObject.toString() + "\n";
							OutputStream os = getOutputStream(request.getSocket());
							os.write(strSend.getBytes("UTF-8"));
							os.flush();
							//尝试消息失败后，删除消息请求
							
							
							break;
						}
						catch(Exception e)
						{
							String strerr = String.format("send message error.server[%s] port[%d]",
									request.getSocket().getLocalAddress().getHostAddress(),request.getSocket().getPort());
							log.error(strerr,e);
							Thread.sleep(2000);
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
			}
			
			for(Integer delqueue : removeList)
			{
				RequestMsgInternal request = requestMap.get(delqueue);
				MessageQueue.getInstance().Remove(delqueue);
				log.debug("45002-MSG-["+request.getServer()+"] send failure,close connect!");
			}
		}
		catch (Exception e)
		{
			log.error("异常.原因:", e);
		}
	}
	
	/**
	 * 
	 * @param socket
	 * @return
	 * @throws IOException
	 */
	 //private PrintWriter getWriter(Socket socket)throws IOException{
	 //	return new PrintWriter(socket.getOutputStream());
	 //}
	 
	 private OutputStream getOutputStream(Socket socket)throws IOException{
			return socket.getOutputStream();
		}
}
