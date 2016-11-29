package com.turk.Service;

import java.rmi.RemoteException;

import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.log4j.Logger;

import com.turk.util.FTPInfo;
import com.turk.util.LogMgr;

public class LoaderService {

	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	private static final String LODERURL="http://192.168.0.20:8888/WebServiceTest/services/OpenPlatform";
	Call _callLoder;
	Service _serviceLoder;
	public LoaderService(){
		_serviceLoder=new Service();
		try {
			_callLoder= (Call)_serviceLoder.createCall();
		
			_callLoder.setTargetEndpointAddress(new java.net.URL(LODERURL));//(LODERURL);
			_callLoder.setOperationName(new QName("http://Utele","loader"));
			_callLoder.addParameter(new QName("http://Utele","in0"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.addParameter(new QName("http://Utele","in1"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.addParameter(new QName("http://Utele","in2"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.addParameter(new QName("http://Utele","in3"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.addParameter(new QName("http://Utele","in4"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.addParameter(new QName("http://Utele","in5"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.addParameter(new QName("http://Utele","in6"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.addParameter(new QName("http://Utele","in7"), org.apache.axis.Constants.XSD_STRING, javax.xml.rpc.ParameterMode.IN);
			_callLoder.setReturnType(org.apache.axis.Constants.XSD_STRING);//设置返回类型  
			//_callLoder.setSOAPActionURI("http://192.168.0.20:8888/WebServiceTest/services/OpenPlatform/loader");
			_callLoder.setUseSOAPAction(true);
			_callLoder.setTimeout(3000);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public String GetLoader(String ftpServer,String ftpPort,
			String user,String password,String passiveMode,
			String encoding,String remoteRoot,String filelist) throws InterruptedException{
		String result;
		try {
			result=(String)_callLoder.invoke(new Object[]{ftpServer,ftpPort,user,password,passiveMode,encoding,remoteRoot,filelist});
			return result;
		} catch (RemoteException e) {
			
			log.error("WebService[GetLoader]调用失败，重连尝试");
			log.error(e);
			try {
				result=(String)_callLoder.invoke(new Object[]{ftpServer,ftpPort,user,password,passiveMode,encoding,remoteRoot,filelist});
			} catch (RemoteException e1) {
				Thread.sleep(1000);
				log.error("WebService[GetLoader]重新调用失败");
				log.error(e1);
				result = "";
			}
			return result;
		}
	}
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		FTPInfo info = new FTPInfo();
 		info.setIP("192.168.0.200");
 		info.setPort(21);
 		info.setUser("ftp");
 		info.setPwd("ftp");
 		LoaderService service=new LoaderService();
		String returnResult=service.GetLoader(info.getIP(), info.getPwd(), info.getUser(),
				info.getPwd(), "true", "UTF-8", "/a", "20110623_20_CDMA1X.txt");
		System.out.println(returnResult);
	}

}
