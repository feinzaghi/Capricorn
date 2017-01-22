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
		//���÷�������NIO�߳���
		EventLoopGroup bossGroup = new NioEventLoopGroup();
		EventLoopGroup workerGroup = new NioEventLoopGroup();
		try {
			ServerBootstrap b = new ServerBootstrap();
//			b.group(bossGroup,workerGroup)
//					.channel(NioServerSocketChannel.class)  //����NIO���͵�channel
//					//.option(ChannelOption.SO_BACKLOG, 1024) //
//					.childHandler(new ChildChannelHandler()); //�����ӵ���ʱ����һ��channel
			
			b.group(bossGroup,workerGroup);
		    b.channel(NioServerSocketChannel.class);// ����nio���͵�channel
		    b.localAddress(new InetSocketAddress(port));// ���ü����˿�
		    b.childHandler(new ChildChannelHandler());
		  //�󶨶˿ڣ�ͬ���ȴ��ɹ�
			ChannelFuture f = b.bind().sync();
			
			Logger.getLogger(NettyServer.class).info(" started and listen on " 
					+ f.channel().localAddress());
			
			//�ȴ������������˿ڹر�
			f.channel().closeFuture().sync();
			
		} finally {
			//�����˳����ͷ��̳߳���Դ
			bossGroup.shutdownGracefully();
			workerGroup.shutdownGracefully();
		}
	}
	
	private class ChildChannelHandler extends ChannelInitializer<SocketChannel> {

		@Override
		protected void initChannel(SocketChannel ch) throws Exception {
			// TODO Auto-generated method stub
			// pipeline����channel�е�Handler����channel���������һ��handler������ҵ��
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
