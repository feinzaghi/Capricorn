package com.turk.rpc;


import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

public class NettyClient {
	
	private IMessageCallBack _backmsg;
	public NettyClient(IMessageCallBack backmsg){
		_backmsg = backmsg;
	}
	
	public void connect(int port,String host,String msg) throws Exception{
		EventLoopGroup group = new NioEventLoopGroup();
		try {
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class)
				.option(ChannelOption.TCP_NODELAY, true)
				.handler(new ClientChannel(msg,_backmsg));

			// 发起异步连接操作
			ChannelFuture f = b.connect(host, port).sync();
			f.addListener(new ChannelFutureListener() {
			        
			        public void operationComplete(ChannelFuture future) throws Exception {
			          if(future.isSuccess()){
			            System.out.println("client connected");
			          }else{
			            System.out.println("server attemp failed");
			            future.cause().printStackTrace();
			          }
			          
			        }
			 });
			f.channel().closeFuture().sync();
		} finally {
			// 优雅退出，释放NIO线程组
			group.shutdownGracefully();
		}
	}
	
	
	
	public class ClientChannel extends ChannelInitializer<SocketChannel> {
		
		String _msg = "";
		IMessageCallBack _callback;
		public ClientChannel(String msg,IMessageCallBack callback) {
			_msg = msg;
			_callback = callback;
		}
		

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			// TODO Auto-generated method stub
			ch.pipeline().addLast(new NettyClientHandler(_msg, new IMessageCallBack (){

				@Override
				public void ReturnMessage(String msg) {
					// TODO Auto-generated method stub
					_callback.ReturnMessage(msg);
				}}));
		}
	}
	
	public static void main(String[] args) throws Exception {
//		int port = 9527;
//		if (args != null && args.length > 0) {
//			try {
//				port = Integer.valueOf(args[0]);
//			} catch(NumberFormatException e) {
//				
//			}
//		}
//		
//		BackMessage backmsg = new BackMessage();
//		new NettyClient(backmsg).connect(port, "localhost", "QUERY TIME ORDER");
		
		MessageToList backmsg = new MessageToList("localhost",9527,1002);
//		new NettyClient(backmsg).connect(port, "localhost", "QUERY TIME ORDER");
//		Slave s = new Slave("localhost",9527);
//		HashSet<String> IPSet = s.GetIPFilterForLog();
//		HashSet<String> WhiteIPSet = s.GetIPWhiteList();
//		for(String ip :WhiteIPSet) {
//			System.out.println("############WhiteIPSet=" + ip);
//		}
	}

	
}
