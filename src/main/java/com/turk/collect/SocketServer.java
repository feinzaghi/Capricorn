package com.turk.collect;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

import com.turk.parser.Parser;
import com.turk.parser.cdr.hw.CommonFunc;
import com.turk.parser.taurus.MapImsiMsisdn;
import com.turk.parser.taurus.MapModCell;
import com.turk.parser.taurus.MonitorMobileConfig;
import com.turk.parser.taurus.SocketDataParserQueue;

import com.turk.task.CollectObjInfo;
import com.turk.util.Task;
import com.turk.util.ThreadPool;
import com.turk.util.Util;


/**
 * ��Socket����ķ�ʽ�������� For Taurus
 * @author Administrator
 *
 */
public class SocketServer extends Parser {
	
	static Socket server;
	
	char[] head;
	char[] tail;
	char[] middle;
	char[] nextHead;
	char[] nextTail;
	//private CollectObjInfo collectObjInfo = null;
	
	private boolean mFlag = true;
	//private MonitorHit _monitor;
	
	ServerSocket serverSocket;
	
	//int _CityID = 0;
	
	public SocketServer()
	{
		
	}
	
	public SocketServer(CollectObjInfo collectInfo)
	{
		super(collectInfo);
	}
	
	/**
	 * ��������
	 */
 	public void start()
 		throws IOException
    {
 		try 
		{
			//����Socket����
			serverSocket=new ServerSocket(collectObjInfo.getDevPort());
			log.debug("Start Server Socket Port[" + collectObjInfo.getDevPort() + "] listener Success!");
		} catch (IOException e) {
			log.warn("Start Server Socket Port[" + collectObjInfo.getDevPort() + "] listener Failure!",e);
		}
 		
 		while(mFlag)
		{
 			try 
 			{
 				
	 			if(!MapImsiMsisdn.getInstance().Loading && 
						!MapModCell.getInstance().Loading)
				{
	 				
	 				//����һ���������ȴ���������Ϣ�󽫴˼������Ӽ����̳߳������С�
			 		log.debug("Socket start,wait for connect...port:" + serverSocket.getLocalPort());
					Socket socket=serverSocket.accept();
					log.debug("New connection accepted " +
		 					socket.getInetAddress() + ":" +socket.getPort() + " local port:" + serverSocket.getLocalPort());
					
					//�������ӵȴ����ݵĳ�ʱʱ�䣬�����ʱ�䷶Χ��û�����ݴ��䣬��ʱ�˳�
					socket.setSoTimeout(this.getCollectObjInfo().getShellTimeout()*1000);//30 �볬ʱ
					
					//����socket��������
					SocketTask socketTask = new SocketTask(socket,"",
							collectObjInfo,this);
					
					ThreadPool.getInstance().addTask(socketTask);
				}
			
				Thread.sleep(1000L);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				errorlog.error("SocketServer thread interrupted err",e);
			} catch (Exception e) {
				errorlog.error("SocketServer err",e);
			}
		}
 		
 		log.debug("Stop taurus socket server.");
    }
 	
 	
 	public String handSql(String[] msg)
	{
		return "";
	}
 	
 	class SocketTask extends Task{
 		
 		private CollectObjInfo collectObjInfo = null;
 		
 		private String mSocketMsg = "";
 		
 		private SocketServer mServer = null;
 		
 		private Socket socket=null;
 		
 		private boolean IsStoped = ThreadPool.getInstance().stopAllTask;
 		
 		Handler handler = null;
 		
 		/**
 		 * 
 		 */
 		private long LoadMonitorDataTime = 0L;
 		
 		
 		public SocketTask(Socket obj,String msg,CollectObjInfo taskinfo,SocketServer server)
 		{
 			socket = obj;
 			mSocketMsg = msg;
 			collectObjInfo = taskinfo;
 			mServer = server;
 		}

 		public void run()
 		{
 			try {
 					//���ֳ�����
 					LoadMonitorDataTime = new Date().getTime();
 					
 					handler = new Handler(socket,mSocketMsg,
								collectObjInfo,mServer);
 					while(!IsStoped)
 					{
 						if(socket!=null && socket.isClosed())
 						{
 							log.debug("client is closed!");
 							return;
 						}
 						
 						handler.run();
 						long curTime = new Date().getTime();
 						if(curTime - LoadMonitorDataTime > 15*60*1000L)
 						{//15������һ��Ԥ�������������
 							
 							try
 							{
 								MonitorMobileConfig.getInstance().Clear();
 							}catch (Exception e) {
 			 					// TODO �Զ����� catch ��
 			 					log.error("Clear monitor config error",e);
 			 				}
 							log.debug("Clear Monitor config." + socket.getInetAddress() + ":" +socket.getPort());
 							LoadMonitorDataTime = new Date().getTime();
 						}
 						
 					}
 					
 					log.debug("process stopped.  connection:" +
		 					socket.getInetAddress() + ":" +socket.getPort() + " local port:" + serverSocket.getLocalPort());
 					
 				} catch (Exception e) {
 					// TODO �Զ����� catch ��
 					errorlog.error("Socket Parser Error",e);
 				}
 			
 		}
 		
 		
		@Override
		public String info() {
			// TODO Auto-generated method stub
			String submittime = "N/A"; 
			String begintime = "N/A";
			if(this.getSubmitTime()!=null)
			{
				submittime = Util.getDateString_Standard_ss(this.getSubmitTime());
			}
			
			if(this.getBeginExceuteTime()!=null)
			{
				begintime = Util.getDateString_Standard_ss(this.getBeginExceuteTime());
			}
			return "Socket Parser:" +
 					socket.getInetAddress() 
 					+ ":" +socket.getPort() 
 					+ " local port:"+collectObjInfo.getDevPort()
 					+ " Commit Time:" + submittime
 					+ " Start Time:" 
 					+ begintime;
		}

		@Override
		protected boolean needExecuteImmediate() {
			// TODO Auto-generated method stub
			return true;
		}

		@Override
		public void stopTask() {
			// TODO Auto-generated method stub
			IsStoped = true;
			
			if(handler!=null)
				handler.Stop();
			
			try {
			if(socket!=null)
					socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}

		@Override
		public Task taskCore() throws Exception {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		protected boolean useDb() {
			// TODO Auto-generated method stub
			return false;
		}
 		
		
		public String toString()
		{
			return "Task-Socket";
		}
 	
 	}
 	
 	/**
 	 * 	single client handler
 	 * @author Administrator
 	 *
 	 */
 	class Handler/*implements Runnable*/{   //�����뵥���ͻ���ͨ��
 		private Socket socket;
 		private SocketServer thisparser;
 		private CollectObjInfo collectObjInfo = null;
 		//private long nStartTime = 0L;
 		private SocketDataParserQueue dataParser = null;
 		
 		private String lastmsgheader = "";
 		private boolean blStop = false;
 		private String lastrow = "";
 		
 		private int nCount = 0;
 		
 		int _CityID = 0;
 		
 		/**
 		 * �ַ�ģ����
 		 */
 		int _DisTableID = -1;
 		
 		public boolean m_Complate = false;
 		  
 		public Handler(Socket skt,String infomation,
 				CollectObjInfo taskInfo,
 				SocketServer parser){
 			this.socket = skt;
 			this.collectObjInfo = taskInfo;
 			this.thisparser = parser;
 			m_Complate = false;
 			dataParser = new SocketDataParserQueue(collectObjInfo,this.thisparser);
 			dataParser.setWriteCommitLog(false);
 			//nStartTime = new Date().getTime();
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
 		
 		/**
 		 * �õ��Է���������Ϣ
 		 * @param socket
 		 * @return
 		 * @throws IOException
 		 */
 		//private BufferedReader getReader(Socket socket)throws IOException{
 		//	return new BufferedReader(new InputStreamReader(socket.getInputStream()));
 		//}
 		
 		/**
 		 * �õ��Է���������Ϣ[��������Taurus��Ŀ]
 		 * @param socket
 		 * @return
 		 * @throws IOException
 		 */
 		private InputStream getInputStream(Socket socket)throws IOException{
 			return socket.getInputStream();
 		}
 		
 		
 		/**
 		 * �õ��Է���������Ϣ[��������Taurus��Ŀ]
 		 * @param socket
 		 * @return
 		 * @throws IOException
 		 */
 		private OutputStream getOutputStream(Socket socket)throws IOException{
 			return socket.getOutputStream();
 		}
 		  
 		public void run()
 		{//����socket��Ϣ������Ϣ�ﵽһ�������󣬷����̳߳ش���������˹��̼���������Ϣ
 			try 
 			{
 				InputStream in = getInputStream(socket);
 				OutputStream out = getOutputStream(socket);
 				//SDTP��Ϣ�ṹ������ʾ��
 				//��Ŀ	˵��
 				//Message Header	��Ϣͷ(������Ϣ������ͷ)
 				//Message Body	��Ϣ�壬���������
 				//��Ϣͷ��Message Header���İ��������ֶΣ�����SDTP�ӿڣ����������ֶα��
 				//�ֶ���	�ֽ���	����	����
 				//TotalLength	2	Unsigned Integer	��Ϣ�ܳ���(����Ϣͷ����Ϣ��)
 				//MessageType	2	Unsigned Integer	��Ϣ����
 				//SequenceId	4	Unsigned Integer	��������ˮ�ţ�˳���ۼӣ�����Ϊ1��ѭ��ʹ�ã�һ��������һ�������Ӧ����Ϣ����ˮ�ű�����ͬ��
 				//TotalContents	1	Unsigned Integer	��Ϣ���е��¼����������40����
 				//������ʵʱ��Ҫ�󣬿�ÿ��ֻ��һ���¼�
 				
 				int startpos = 0;
 				
 				int len = 0;
 				//get msg header
 				byte[] readbuff = null;
 				
 				byte[] buffHead = new byte[9];
 				
 				int nReadBytes = 9;
 				do
 				{
 					readbuff = new byte[nReadBytes];
 					len = in.read(readbuff);
 					if(len<0)
 					{
 						try
 						{
 							Thread.sleep(1);
 						}catch(Exception e){
 							log.error(e);
 						}
 						if(blStop)
 							return;
 						continue;
 					}
 					System.arraycopy(readbuff, 0, buffHead, startpos, len);
 					startpos = startpos + len;
 					nReadBytes = nReadBytes - len;
 				}while(nReadBytes > 0);
 				
 				startpos = 0;
 				String test = CommonFunc.bytesToHexString(buffHead);
 				
 				//log.debug("msg header:" + test);
 				byte[] temp = new byte[2];
 				System.arraycopy(buffHead, startpos, temp, 0, temp.length);
 				int TotalLength = Integer.parseInt(CommonFunc.bytesToHexString(temp),16);//CommonFunc.bytesToIntNew(temp);
 				startpos= startpos + 2;
 				
 				//log.debug("TotalLength:" + TotalLength);
 				if(TotalLength==0)
 				{
 					socket.close();
 					log.warn("��Ϣ����Ϊ0�����½�������");
 					return;
 				}
 				
 				temp = new byte[2];
 				System.arraycopy(buffHead, startpos, temp, 0, temp.length);
 				int MessageType = Integer.parseInt(CommonFunc.bytesToHexString(temp),16);
 				startpos= startpos + 2;		
 				
 				temp = new byte[4];
 				System.arraycopy(buffHead, startpos, temp, 0, temp.length);
 				int SequenceId = CommonFunc.byteToint(temp);
 				startpos= startpos + 4;
 				
 				//log.debug("SequenceId:" + SequenceId);
 				
 				temp = new byte[1];
 				System.arraycopy(buffHead, startpos, temp, 0, temp.length);
 				int TotalContents = CommonFunc.byteToint(temp);
 				startpos= startpos + 1;
 				
 				//log.debug("TotalContents:" + TotalContents);
 				
 				int lastLength = TotalLength - 9;
 				byte[] buffBody = null;
 				
 				
 				//log.debug("header msg:" + test + " length[" + TotalLength + "]");


 				switch(MessageType)
 				{
 					case 3:
 						if(lastLength > 0)
 		 				{//�Ƚ���������ڴ�
 			 				buffBody = new byte[lastLength];
 							in.read(buffBody);
 		 				}
 						log.debug("linkCheck_Req:" + test);
 						out.write(CommonFunc.shortToByteArray((short)TotalLength));
 						out.write(CommonFunc.hexStringToByte("8003"));
 						out.write(CommonFunc.int2byte(SequenceId));
 						out.write(new byte[]{1});
 						out.flush();
 						break;
 					case 6:
 						//notifyEventData_Req	0x0006	ҵ����������֪ͨ����
 						//notifyEventData_Resp	0x8006	ҵ����������֪ͨӦ��
 						
 						if(lastLength <= 0)
 		 				{//�Ƚ���������ڴ�
 			 				//buffBody = new byte[lastLength];
 							//in.read(buffBody);
 							log.debug("msgheader:["+test+"] msg length <=0 " + socket.getInetAddress() + ":" +socket.getPort());
 							break;
 		 				}
 						
 						byte[] eventhead = new byte[10];
 						startpos = 0;
 						nReadBytes = 10;
 		 				do
 		 				{
 		 					readbuff = new byte[nReadBytes];
 		 					len = in.read(readbuff);
 		 					if(len<0)
 		 					{
 		 						try
 		 						{
 		 							Thread.sleep(1);
 		 						}catch(Exception e){
 		 							log.error(e);
 		 						}
 		 						if(blStop)
 		 							return;
 		 						continue;
 		 					}
 		 					System.arraycopy(readbuff, 0, eventhead, startpos, len);
 		 					startpos = startpos + len;
 		 					nReadBytes = nReadBytes - len;
 		 				}while(nReadBytes > 0);
 						
 						startpos = 0;
 						
 						String hex = CommonFunc.bytesToHexString(eventhead);
 						
 						temp = new byte[2];
 						System.arraycopy(eventhead, startpos, temp, 0, temp.length);
 						
 						
 						int MobileEventType = Integer.parseInt(CommonFunc.bytesToHexString(temp),16);
 						//log.debug("MobileEventType��" + MobileEventType);
 							
 						startpos= startpos + 2;
 							
 						temp = new byte[8];
 						System.arraycopy(eventhead, startpos, temp, 0, temp.length);
 					
 						startpos= startpos + 8;
 					  	
 						lastLength = lastLength - 10;
 						byte[] buff = null;
 						String remainingData = "";
 						
 						//����ʣ�೤�ȵ�buff
 						
 						len = 0;
						do{//ѭ��ȡ����ֱ��ȡ��Ϊֹ
							buff = new byte[lastLength];
							len = in.read(buff);
							if(len<0)
 		 					{
 		 						try
 		 						{
 		 							Thread.sleep(1);
 		 						}catch(Exception e){
 		 							log.error(e);
 		 						}
 		 						if(blStop)
 		 							return;
 		 						continue;
 		 					}
							remainingData = remainingData + new String(buff,0,len);
							lastLength = lastLength - len;
						}
						while(lastLength>0);
 						
 					  	out.write(CommonFunc.shortToByteArray((short)10));
	 					out.write(CommonFunc.hexStringToByte("8006"));
	 					out.write(CommonFunc.int2byte(SequenceId));
	 					out.write(new byte[]{1});
	 						
 					  	if(TotalContents > 1)
 					  	{
 					  		log.debug("Length="+ TotalLength 
 	 					  			+" ROW="+ TotalContents
 	 					  			+" msgheader[" + test +"] " + socket.getInetAddress() + ":" +socket.getPort());
 					  		
 					  		out.write(new byte[]{1});
 					  		out.flush();
 					  		socket.close();
 		 					log.warn("msgheader err:" + test);
 		 					
 		 					String strdebug = String.format("header last msg:[%s] current[%s] last body[%s]", 
 									lastmsgheader,test,lastrow);
 		 					log.debug(strdebug);
 		 					
 					  	}else
 					  	{
 					  		
 					  		dataParser.AddMsgMap(MobileEventType, remainingData,hex);
 					  		out.write(new byte[]{1});
 					  		out.flush();
 					  		nCount++;
 					  		
 					  		if(nCount > 200)
 					  		{//200�м�¼����һ��
 					  			ThreadPool.getInstance().addTask(dataParser);
 					  			dataParser = new SocketDataParserQueue(collectObjInfo,this.thisparser);
 					  			dataParser.setWriteCommitLog(false);
 					  			
 					  			nCount = 0;
 					  		}
 					  	}
 					  	
 					  	lastrow = remainingData;
 						break;
 					default:
 						
 						buff = new byte[lastLength];
 						len = 0;
						do{
							len = in.read(buff);
							if(len<0)
							{
								if(blStop)
		 							return;
								continue;
							}
							lastLength = lastLength - len;
							if(lastLength>0)
							{
								buff = new byte[lastLength];
							}
						}
						while(lastLength>0);
						
 						String strdebug = String.format("header last msg:[%s] current[%s] " +
 								" last body[%s]", lastmsgheader,test,lastrow);
		 				log.debug(strdebug);
 						break;
 				}
 				lastmsgheader = test;
 				
 			}catch (Exception e) {
 				errorlog.error("Server,������Ϣ�쳣:close socket-"
 							+ socket.getInetAddress() + ":" 
 							+ socket.getPort() + " local port:" 
 							+ serverSocket.getLocalPort(),e);
 				  try {
					socket.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					errorlog.error("Socket error.",e1);
				}
 				  //pw.println(e.getMessage());//���ؿͻ�����Ϣ
 			}finally {
 				//if(pw!=null)
				//	  pw.close();
 				//log.debug("MSG-Complate");
 				m_Complate = true;
 			}
 			
 			
 		}
 		
 		
 		public void Stop()
 		{
 			blStop = true;
 			if(socket!=null)
 			{
 				try {
					socket.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
 			}
 		}
 	}
 	
 	
	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		start();
		return true;
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		this.mFlag = false;
		if(serverSocket!=null)
		{
			try {
				serverSocket.close();
				log.debug("Stop taurus socket object.");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				errorlog.error("Socket close err",e);
			}
		}
	}
}
	

