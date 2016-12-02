package com.turk.socket;
import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.*;

import org.mortbay.log.Log;

public class Server {

	
	
	private int port = 9001;
	ServerSocket serverSocket;
	Object _obj = null;
	public Server(Object obj){
		try {
			
			serverSocket=new ServerSocket(port);
			this._obj = obj;
			
			Log.info("服务器启动!");
		} catch (IOException e) {
			Log.warn("服务器连接失败!");
			e.printStackTrace();
		}
	}
	
	public void service(String msg)
	{
		while(true)
		{
			Socket socket=null;
			try {
				socket=serverSocket.accept();
				Thread workThread=new Thread(new Handler(socket,msg,_obj));
				workThread.start();
			} catch (IOException e) {
				// TODO 自动生成 catch 块
				e.printStackTrace();
			}
		}
	}
}

class Handler implements Runnable{   //负责与单个客户的通信
	  private Socket socket;
	  private String infomation;
	  private Object obj;
	  
	  public Handler(Socket socket,String infomation,Object obj){
		  this.socket=socket;
		  this.infomation=infomation;
		  this.obj = obj;
	  }
	  private PrintWriter getWriter(Socket socket)throws IOException{
		  return new PrintWriter(socket.getOutputStream());
	  }
	  private BufferedReader getReader(Socket socket)throws IOException{
		  return new BufferedReader(new InputStreamReader(socket.getInputStream()));
	  }
	  
	  public void run()
	  {
		  try 
		  {
		      System.out.println("New connection accepted " +
		      socket.getInetAddress() + ":" +socket.getPort());
		      BufferedReader br = getReader(socket);
		      PrintWriter pw = getWriter(socket);
	     
		      String msg = br.readLine(); //收到客户端信息
		      //while ((msg = br.readLine()) != null) { //接收和发送数据，直到通信结束
		      System.out.println("客户端输出信息："+msg);
		      pw.println("成功!");//返回客户端信息
		      pw.close();
		      socket.close();
		      
		      SocketMethod mtd = (SocketMethod)obj;
		      mtd.ReturnMsg(msg);
		      
		      // }
		  }catch (IOException e) {
	       e.printStackTrace();
		  }finally {
			  try{
				  if(socket!=null)socket.close(); //断开连接
			  }catch (IOException e) 
			  {
				  e.printStackTrace();
			  }
		  }
	  }
	  
	  public Object invoke(Object proxy, Method m, Object[] args) throws Throwable
	  {
	        Object result;
	        try
	        {
	            //自定義的處理
	            System.out.println("--before method " + m.getName());
	            //調用GreetImpl中方法
	            result = m.invoke(obj, args);
	        }
	        catch(InvocationTargetException e)
	        {
	            throw e.getTargetException();
	        }
	        catch(Exception e)
	        {
	            throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
	        }
	        finally
	        {
	            System.out.println("--after method " + m.getName());
	        }
	        return result;
	 }
}
