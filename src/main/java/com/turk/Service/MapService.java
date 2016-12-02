package com.turk.Service;

import java.rmi.RemoteException;

import javax.xml.namespace.QName;
import javax.xml.rpc.ServiceException;

import org.apache.axis.client.Call;
import org.apache.axis.client.Service;
import org.apache.log4j.Logger;
import org.apache.axis.encoding.ser.ArrayDeserializerFactory;
import org.apache.axis.encoding.ser.ArraySerializerFactory;
import org.apache.axis.encoding.ser.BeanDeserializerFactory;
import org.apache.axis.encoding.ser.BeanSerializerFactory;
import org.apache.axis.types.Schema;

import com.turk.config.SystemConfig;
import com.turk.util.LogMgr;

public class MapService {
	private static String _ServiceUrl = SystemConfig.getInstance().UteleServiceUrl();
	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	
	Call _callRegion;
	Service _serviceRegion; 
	
	Call _callCell;
	Service _serviceCell;
	
	 Service _TimeTestService;
	 Call _TimeTestCall;
	
	
	public MapService()
	{
		try {
			_serviceRegion = new Service();
			_callRegion = (Call)_serviceRegion.createCall();
			_callRegion.setTargetEndpointAddress(_ServiceUrl);
			_callRegion.setOperationName(new QName("http://utele.org/", "RegionName"));
			_callRegion.addParameter(new QName("http://utele.org/","CityID"), org.apache.axis.Constants.XSD_STRING,
                      javax.xml.rpc.ParameterMode.IN);//接口的参数
			_callRegion.addParameter(new QName("http://utele.org/","x"), org.apache.axis.Constants.XSD_STRING,
        			javax.xml.rpc.ParameterMode.IN);//接口的参数
			_callRegion.addParameter(new QName("http://utele.org/","y"), org.apache.axis.Constants.XSD_STRING,
        			javax.xml.rpc.ParameterMode.IN);//接口的参数
			_callRegion.setReturnType(org.apache.axis.Constants.XSD_STRING);//设置返回类型  
			_callRegion.setSOAPActionURI("http://utele.org/RegionName");
			_callRegion.setTimeout(30000);
			
			
			_serviceCell = new Service();
			_callCell = (Call)_serviceCell.createCall();
			_callCell.setTargetEndpointAddress(_ServiceUrl);
			_callCell.setOperationName(new QName("http://utele.org/", "CellInfo"));
			_callCell.addParameter(new QName("http://utele.org/","CityID"), org.apache.axis.Constants.XSD_STRING,
                      javax.xml.rpc.ParameterMode.IN);//接口的参数
			_callCell.addParameter(new QName("http://utele.org/","x"), org.apache.axis.Constants.XSD_STRING,
        			javax.xml.rpc.ParameterMode.IN);//接口的参数
			_callCell.addParameter(new QName("http://utele.org/","y"), org.apache.axis.Constants.XSD_STRING,
        			javax.xml.rpc.ParameterMode.IN);//接口的参数
			_callCell.setReturnType(org.apache.axis.Constants.XSD_STRING);//设置返回类型  
			_callCell.setSOAPActionURI("http://utele.org/CellInfo");
			_callCell.setTimeout(30000);
			
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	} 
	
	public String GetRegion(Integer CityID,double x,double y) throws InterruptedException
	{
		String temp1 = CityID.toString();
    	String temp2 = String.format("%f", x);
    	String temp3 = String.format("%f", y);
    	String result;

		try {
			
        	result = (String)_callRegion.invoke(new Object[]{temp1,temp2,temp3});
        	
        	return result;
		 } catch (RemoteException e) {
				// TODO Auto-generated catch block
				log.error("WebService[GetRegion]调用失败，重连尝试");
				log.error(e);
				try {
					Thread.sleep(1000);
					result = (String)_callRegion.invoke(new Object[]{temp1,temp2,temp3});
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					log.error("WebService[GetRegion]重新调用失败");
					log.error(e1);
					result = "";
				}
				return result;
			}
	}
	
	/**
	 * 获取小区信息
	 * @param CityID
	 * @param x
	 * @param y
	 * @return
	 * @throws InterruptedException
	 */
	public String GetCellInfo(Integer CityID,double x,double y) throws InterruptedException
	{
		String temp1 = CityID.toString();
    	String temp2 = String.format("%f", x);
    	String temp3 = String.format("%f", y);
    	String result;
    	
		try {
        	result = (String)_callCell.invoke(new Object[]{temp1,temp2,temp3});
        	return result;
		 } catch (RemoteException e) {
				// TODO Auto-generated catch block
				log.error("WebService[GetCellInfo]调用失败，重连尝试");
				log.error(e);
				try {
					Thread.sleep(1000);
					result = (String)_callCell.invoke(new Object[]{temp1,temp2,temp3});
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					log.error("WebService[GetCellInfo]重新调用失败");
					log.error(e1);
					result = "";
				}
				return result;
		 }
		 finally
		 {
			 _callCell = null;
			 _serviceCell = null;
		 }
	}
	
	public QueryResult TimeTest()
	{
		try {
			Service TestService = new Service();
			Call TestCall = (Call)TestService.createCall();
			 
			
			//注册返回对象类型
			QName qn = new QName("QueryResult");    
			TestCall.registerTypeMapping(QueryResult.class, qn,    
					new BeanSerializerFactory(QueryResult.class, qn),    
					new BeanDeserializerFactory(QueryResult.class, qn));
			QName qn1 = new QName("Schema");    
			TestCall.registerTypeMapping(Schema.class, qn1,    
					new BeanSerializerFactory(Schema.class, qn1),    
					new BeanDeserializerFactory(Schema.class, qn1));
			
			//http://www.w3.org/2001/XMLSchema
			QName qName1 = new QName("http://zctt.org/","ArrayOfString");
			
			TestCall.registerTypeMapping(ArrayOfString.class, qName1,
					new BeanSerializerFactory(ArrayOfString.class, qName1),
					new BeanDeserializerFactory(ArrayOfString.class, qName1));
			
			
			///TestCall.addParameter(new QName("http://zctt.org/","paras"), 
			//		org.apache.axis.encoding.XMLType.XSD_STRING,
	        //   javax.xml.rpc.ParameterMode.IN);
		
			
			TestCall.addParameter(new QName("http://zctt.org/","paras"), 
					qName1,
	           javax.xml.rpc.ParameterMode.IN);
			
			
			
			ArrayOfString arr = new ArrayOfString();
			String[] str = new String[] {"uuu","dddd"};
			arr.setString(str);
			
			
			//访问WebService接口
			TestCall.setTargetEndpointAddress("http://localhost:1436/SharePlat.asmx?WSDL");
			TestCall.setOperationName(new QName("http://zctt.org/", "TestReturnObject"));
			
			TestCall.setSOAPActionURI("http://zctt.org/TestReturnObject");
			TestCall.setTimeout(30000);
			TestCall.setReturnClass(QueryResult.class); //设置返回对象类型
	
			
			int i = 1123;
		try {
			QueryResult result = (QueryResult)TestCall.invoke(new Object[]{arr});
        	return result;
		 } catch (RemoteException e) {
			 e.printStackTrace();
		 }
		} catch (ServiceException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		return null;
	}
	
	public QueryResult WSDLTest()
	{
		QueryResult result = new QueryResult();
		/*
		RPCServiceClient serviceClient = new RPCServiceClient();

        Options options = serviceClient.getOptions();

        EndpointReference targetEPR = new EndpointReference(
                "http://127.0.0.1:8080/axis2/services/MyService");
        options.setTo(targetEPR);

        // /////////////////////////////////////////////////////////////////////

        
         * Creates an Entry and stores it in the AddressBook.
         

        // QName of the target method 
        QName opAddEntry = new QName("http://webservice.rp.mid.com/xsd", "createResourceTemplate");

        
         * Constructing a new Entry
         
        AddResourceTemplateRequestMsg entry = new AddResourceTemplateRequestMsg();
        entry.setResourceTemplateID("testtttt") ;
        entry.setTransactionId("testaaaaa") ;

         此处可换成String数组传入即可。
        Object[] opAddEntryArgs = new Object[] { entry };
        Class[] returns = new Class[]{AddResourceTemplateResponseMsg.class} ;
        Invoking the method
        Object[] obj = serviceClient.invokeBlocking(opAddEntry, opAddEntryArgs, returns) ;
        */
		return result;
	}
	
	
	 public static void main(String[] args) {
		 
	
		
	 }
}
