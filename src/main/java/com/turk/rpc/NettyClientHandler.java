package com.turk.rpc;

import org.apache.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.CharsetUtil;

public class NettyClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
	private static final Logger logger = Logger.getLogger(NettyClientHandler.class);
	
	private final ByteBuf firstMessage;
	
	IMessageCallBack _callback;
	
	public NettyClientHandler(String msg,IMessageCallBack callback){
//		byte[] req = msg.getBytes();
		firstMessage = Unpooled.copiedBuffer(msg,CharsetUtil.UTF_8);
		//firstMessage.writeBytes(req);
		_callback = callback;
	}
	
	public void channelActive(ChannelHandlerContext ctx) {
//	    ctx.write(Unpooled.copiedBuffer("QUERY TIME ORDER", CharsetUtil.UTF_8));
		//发送消息到服务端
	    ctx.writeAndFlush(firstMessage);
	}
	

	
	public void exceptionCaught(ChannelHandlerContext ctx,Throwable cause) {
		logger.warn("Unexpected exception from downstream : " + cause.getMessage());
		System.out.println("Unexpected exception from downstream : " + cause.getMessage());
		ctx.close();
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg)
			throws Exception {
		// TODO Auto-generated method stub
		//客户端收到服务端的返回消息
		Logger.getLogger(NettyClientHandler.class).info("Recive Message from Server");
		ByteBuf buf = (ByteBuf) msg;
		byte[] req = new byte[buf.readableBytes()];
		buf.readBytes(req);
		String body = new String(req, "UTF-8");
		//System.out.println("Now is :" + body);
		_callback.ReturnMessage(body);
		Logger.getLogger(NettyClientHandler.class).info("Callback to my host:" + body);
		ctx.disconnect();
	}
	
	@Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.fireChannelReadComplete();
    }

}
