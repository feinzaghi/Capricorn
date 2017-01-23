package com.turk.rpc;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;

import net.sf.json.JSONObject;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;

import com.turk.alarm.AlarmMgr;
import com.turk.alarm.ProcessStatus;
import com.turk.app.appinterface;
import com.turk.clusters.master.TaskManage;
import com.turk.clusters.model.Register;
import com.turk.clusters.model.SlaveInfo;
import com.turk.clusters.model.StatTaskInfo;
import com.turk.clusters.model.TaskMsg;
import com.turk.clusters.slave.IExecute;
import com.turk.clusters.slave.SlaveActive;
import com.turk.clusters.slave.SlaveConfig;
import com.turk.clusters.slave.StatTaskExecute;
import com.turk.clusters.slave.TaskExecute;
import com.turk.config.SystemConfig;
import com.turk.console.ConsoleMgr;
import com.turk.datalog.DataLogMgr;
import com.turk.db.GPDBPool;
import com.turk.framework.DataLifecycleMgr;
import com.turk.framework.Factory;
import com.turk.socket.Client;
import com.turk.task.TaskMgr;
import com.turk.util.CommonDB;
import com.turk.util.DbPool;
import com.turk.util.LogMgr;
import com.turk.util.ThreadPool;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Server 端处理
 * @author Administrator
 *
 */
public class SlaveNettyServerHandler extends ChannelInboundHandlerAdapter{

	private Logger log = Logger.getLogger(SlaveNettyServerHandler.class);
	
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		//接收到client消息
		ByteBuf buf = (ByteBuf) msg;
		byte[] req = new byte[buf.readableBytes()];
		buf.readBytes(req);
		String msgBody = new String(req,"UTF-8");
//		
		JSONObject jsonObject = JSONObject.fromObject(msgBody); 
		Object bean = JSONObject.toBean(jsonObject);
		int MsgID = 0;
		try 
		{
			assertEquals(jsonObject.get("msgID"),
					PropertyUtils.getProperty(bean, "msgID"));
			//
			MsgID = Integer.parseInt(PropertyUtils.getProperty(bean, "msgID").toString());
    		  
		} catch (Exception e) {
			// TODO Auto-generated catch block
			log.error("Master,Json ERROR",e);
		}
//		InetSocketAddress insocket = (InetSocketAddress) ctx.channel()
//                .remoteAddress();
//        String clientIP = insocket.getAddress().getHostAddress();
		
//		
//		//
		String strReturn = "";
		//服务器端消息列表
		//节点接收任务编号列表
		
		  IExecute exe = Factory.createSlaveExecute(MsgID);
		
		  switch(MsgID)
		  {
		      case 2001://采集新任务
		    	  
		    	  if(ThreadPool.getInstance().getThreadQueueCount()>100)
		    	  {
//		    		  pw.println("NO");//返回客户端信息
//				      pw.flush();
//				      return;
		    		  strReturn = "NO";
		    	  }
		    	  TaskMsg obj = new TaskMsg();
		    	  obj = obj.getByJson(msgBody);
		    	  if(obj != null)
		    	  {
			    	  if(exe.Execute(obj))
			    	  {
			    		  log.debug("Task:[" + obj.getTaskID() +"] Start....");	
			    		  TaskMsg task = new TaskMsg();
				  		  task.setMsgID(2003);//通知主控，任务已加入列表，请主控从任务列表中删除该任务
				  		  task.setTaskID(obj.getTaskID());
				  		  task.setObjMap(obj.getObjMap());
				  	  	  JSONObject json = JSONObject.fromObject(task);
				  		    //System.out.println(jsonObject);
				  		  log.debug("2003-MSG:任务已加入列表：" + json.toString());
				  		  Client clt = new Client(SlaveConfig.getInstance().getMasterServer(),
				  		    		SlaveConfig.getInstance().getMasterPort());
				  		  String Result = clt.SendMsgNetty(json.toString());
				  		  if(Result.equals("Done"))
				  		  {
				  		      log.debug("2003-MSG:["+obj.getTaskID()+"] Complete!");
				  		  }
			    	  }
			    	  else
			    	  {
			    		  log.warn("任务执行异常");
			    	  }
		    	  }
		    	  else
		    	  {
		    		  log.warn("任务对象为 null");
		    	  }
		    	  break;
		      case 2003://汇总新任务
		    	  StatTaskInfo objstat = new StatTaskInfo();
		    	  objstat = objstat.getByJson(msgBody);
		    	  if(objstat != null)
		    	  {
			    	  exe.Execute(objstat);
			    	  log.debug("StatTask:[" + objstat.getID() +"] Start....");
		    	  }
		    	  break;
		      case 9999:
		    	  shutdown();
//		    	  pw.println("Done");//返回客户端信息
//			      pw.flush();
		    	  log.debug("Close console service.");
				  ConsoleMgr.getInstance(SystemConfig.getInstance().getCollectPort()).stop();
				  log.debug("exit!");
				  System.exit(0);
		    	  return;
		      default:
		    	  break;	  
		  }
		
		//strReturn = strReturn + "Done";
//		ByteBuf buf = (ByteBuf) msg;
//		byte[] req = new byte[buf.readableBytes()];
//		buf.readBytes(req);
//		String body = new String(req,"UTF-8");
//		System.out.println("The Netty server receive order : " + body);
		
//		String currentTime = "QUERY TIME ORDER".equalsIgnoreCase(body)?new java.util.Date(
//				System.currentTimeMillis()).toString() : "BAD ORDER";
		
				
		//消息返回给客户端
		ByteBuf resp = Unpooled.copiedBuffer(strReturn.getBytes());
		ctx.write(resp);
		
	}
	
	public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
		
		//System.out.println(ctx.name());
	    ctx.fireChannelRegistered();
	}
	
	public void channelReadComplete(ChannelHandlerContext ctx) throws Exception{
		ctx.flush();
	}
	
	public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause){
		Logger.getLogger(SlaveNettyServerHandler.class).info("Unexpected exception from downstream : " + cause.getMessage());
		ctx.close();
	}
	
	 private void shutdown()
	  {
		  log.debug("receive shutdown command");
		  ThreadPool.getInstance().stoptask();//停止已实现stop方法的任务
		  SlaveActive.getInstance().Stop();
		  while(TaskMgr.getInstance().size() > 0)
		  {
			  try 
			  {
					Thread.sleep(10000L);
					log.debug("waiting for stop");
							
			  } catch (InterruptedException e) {
							// TODO Auto-generated catch block
					e.printStackTrace();
			  }
		  }
					
					log.debug("No task running,close thread pool.");

					ThreadPool.getInstance().destroy();

					if(SystemConfig.getInstance().isEnableRunDTStatistic())
					{
						//反射
						try
						{
							appinterface appdt = ((appinterface)Class.forName("DT.DTMain").newInstance()).getInstance();
							appdt.Shutdown();
						}
						catch (Exception e)
						{
							e.printStackTrace();
						}
					}
					
					ProcessStatus.getInstance().stopScan();
					
					DataLifecycleMgr.getInstance().stop();
					AlarmMgr.getInstance().shutdown();
					DataLogMgr.getInstance().commit();
					LogMgr.getInstance().getDBLogger().dispose();
					log.debug("Close DB Pool.");
					CommonDB.closeDbConnection();
					DbPool.close();
					GPDBPool.close();
	  }
}
