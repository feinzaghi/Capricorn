package com.turk.rpc;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;

import net.sf.json.JSONObject;

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.log4j.Logger;

import com.turk.clusters.master.TaskManage;
import com.turk.clusters.model.Register;
import com.turk.clusters.model.SlaveInfo;
import com.turk.clusters.model.TaskMsg;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Server 端处理
 * @author Administrator
 *
 */
public class MasterNettyServerHandler extends ChannelInboundHandlerAdapter{

	private Logger log = Logger.getLogger(MasterNettyServerHandler.class);
	
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
		InetSocketAddress insocket = (InetSocketAddress) ctx.channel()
                .remoteAddress();
        String clientIP = insocket.getAddress().getHostAddress();
		
//		
//		//
		String strReturn = "";
		//服务器端消息列表
		switch(MsgID)
		{
			case 1001://注册信息
				Register reg1 = new Register();
				TaskManage.getInstance().NodeRegister(reg1.getByJson(msgBody));
				break;
			case 1002://报活同步消息
				Register reg2 = new Register();
				TaskManage.getInstance().UpdateSlaveStatus(reg2.getByJson(msgBody));
				break;
			case 1003://节点通知关闭
				Register reg3 = new Register();
				TaskManage.getInstance().UpdateSlaveStatus(reg3.getByJson(msgBody));
				break;
			case 2002://任务完成
				TaskMsg task = new TaskMsg();
				task = task.getByJson(msgBody);
				String server2 = clientIP;
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
			case 2003://通知任务发送成功
				TaskMsg taskruning = new TaskMsg();
				taskruning = taskruning.getByJson(msgBody);
				TaskManage.getInstance().delActiveTask(taskruning.getTaskID(), false);
				String server3 = clientIP;
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
		Logger.getLogger(MasterNettyServerHandler.class).info("Unexpected exception from downstream : " + cause.getMessage());
		ctx.close();
	}
}
