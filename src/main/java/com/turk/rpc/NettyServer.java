package com.turk.rpc;

import java.net.InetSocketAddress;

import org.apache.log4j.Logger;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

public class NettyServer {

	public void bind(int port) throws Exception{
		//配置服务器的NIO线程组
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
//			b.group(bossGroup,workerGroup)
//					.channel(NioServerSocketChannel.class)  //设置NIO类型的channel
//					//.option(ChannelOption.SO_BACKLOG, 1024) //
//					.childHandler(new ChildChannelHandler()); //有连接到达时创建一个channel
			
			b.group(bossGroup,workerGroup);
		    b.channel(NioServerSocketChannel.class);// 设置nio类型的channel
		    b.localAddress(new InetSocketAddress(port));// 设置监听端口
		    b.childHandler(new ChildChannelHandler());
		  //绑定端口，同步等待成功
			ChannelFuture f = b.bind().sync();
			
			Logger.getLogger(NettyServer.class).info(" started and listen on " 
					+ f.channel().localAddress());
			
			//等待服务器监听端口关闭
			f.channel().closeFuture().sync();
			
		} finally {
			//优雅退出，释放线程池资源
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
	
	private class ChildChannelHandler extends ChannelInitializer<SocketChannel> {

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			// TODO Auto-generated method stub
			// pipeline管理channel中的Handler，在channel队列中添加一个handler来处理业务
			ch.pipeline().addLast("NettyMasterServer",new MasterNettyServerHandler());
			ch.pipeline().addLast("NettySlaveServer",new SlaveNettyServerHandler());
		}
		
	}
	
	public static void main(String[] args) throws Exception{
		int port = 9527;
		if(args != null && args.length > 0){
			try{
				port = Integer.valueOf(args[0]);
			}catch (NumberFormatException e){
				
			}
		
		}
		new NettyServer().bind(port);
	}
	
}
