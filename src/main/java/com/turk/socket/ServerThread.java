package com.turk.socket;

public class ServerThread {

	private static ServerThread instance;
	private Thread thread;
	private Server server;
	private ServerThread(){
		server = new Server(null);
	}
	
	public static synchronized ServerThread getInstance(){
		if(instance==null){
			return new ServerThread();
		}
		return instance;
	}
	
	public void start(){
		this.thread=new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				server.service(null);
			}
		});
		thread.start();
	}
}
