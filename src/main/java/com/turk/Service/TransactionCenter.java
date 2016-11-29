package com.turk.Service;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.util.LogMgr;
import com.turk.util.Util;

public class TransactionCenter {

	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	
	private URL url;
    private HttpURLConnection urlconn;

    String inencoding;
    String outencoding;
    
    public TransactionCenter(String inencoding, String outencoding) {
        this.inencoding = inencoding;
        this.outencoding = outencoding;
    }
    
    public String connect(Map<String,String> params, String postUrl) {
    	 String response = "";
    	 log.info("PostUrl:" + postUrl);
    	 HttpClientSample http = new HttpClientSample();
    	 response = http.doPost(postUrl, params);
    		 
    	/*
        BufferedReader br = null;
        String response = "", brLine = "";
        try {
            //params=URLEncoder.encode(params,"GB2312"); //use URLEncoder.encode for encode the params
            url = new URL(postUrl);
            urlconn = (HttpURLConnection) url.openConnection();
            //urlconn.setRequestProperty("user-agent","mozilla/4.7 [en] (win98; i)");    //set request header 
            //urlconn.setRequestProperty("X-Forwarded-For", "127.0.0.1");
            urlconn.setRequestProperty("accept", "gzip,deflate"); 
            urlconn.setRequestProperty("connection", "Keep-Alive"); 
            urlconn.setRequestProperty("user-agent", 
            "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1)"); 
            urlconn.setRequestProperty("Content-Type","text/xml;charset=utf-8");//必须，否则服务器端收取数据有问题
            if(!sessionId.isEmpty())
            	urlconn.setRequestProperty("Cookie", sessionId);
            urlconn.setConnectTimeout(30000);
            urlconn.setReadTimeout(30000);
            
            urlconn.setRequestMethod("POST");     // request method, default GET
            urlconn.setUseCaches(false);    //Post can not user cache
            urlconn.setDoOutput(true);    //set output from urlconn
            urlconn.setDoInput(true);    //set input from urlconn
            
            //urlconn.set
            //urlconn.connect();

            //OutputStreamWriter out = new OutputStreamWriter(urlconn.getOutputStream(), inencoding);  
            //out.write(params);
            //out.flush();
            //out.close();    // output stream close,That's means need not to post data to this outputstream

            DataOutputStream out = new DataOutputStream(urlconn.getOutputStream());
            // The URL-encoded contend
            // 正文，正文内容其实跟get的URL中'?'后的参数字符串一致
            //String content = params;
            
            // DataOutputStream.writeBytes将字符串中的16位的unicode字符以8位的字符形式写道流里面
            byte[] output = Util.objectToBytes(params);
            out.write(output);
            out.flush();
            out.close(); // flush and close
            
            br = new BufferedReader(new InputStreamReader(urlconn.getInputStream(), inencoding));
            while((brLine = br.readLine())!=null)
                response =(new StringBuilder(String.valueOf(response))).append(brLine).toString();
        } catch (Exception e) {
        	log.error(e);
            
        } finally {
            try {
                if(br != null) {
                    br.close();
                }
            } catch (IOException e) {
                log.error("input stream close fail",e);
            }
            urlconn.disconnect();
        }
        */
        return response;
    }
    
    public String connect(String params, String postUrl) {
        BufferedReader br = null;
        String response = "", brLine = "";
        try {
            //params=URLEncoder.encode(params,"GB2312"); //use URLEncoder.encode for encode the params
        	log.info("PostUrl:" + postUrl);
            url = new URL(postUrl);
            urlconn = (HttpURLConnection) url.openConnection();
            //urlconn.setRequestProperty("user-agent","mozilla/4.7 [en] (win98; i)");    //set request header 
            //urlconn.setRequestProperty("X-Forwarded-For", "127.0.0.1");
            urlconn.setRequestProperty("accept", "*/*");
            urlconn.setRequestProperty("Accept-Language", "zh-cn");
            urlconn.setRequestProperty("connection", "Keep-Alive"); 
            urlconn.setRequestProperty("Accept-Encoding", "gzip, deflate");
            urlconn.setRequestProperty("Host", "localhost:8000"); 
            urlconn.setRequestProperty("Content-Type","application/x-www-form-urlencoded");//必须，否则服务器端收取数据有问题
            urlconn.setRequestProperty("Content-Length","20");
            urlconn.setRequestProperty("Cache-Control","no-cache");
            urlconn.setConnectTimeout(30000);
            urlconn.setReadTimeout(30000);
            
            urlconn.setRequestMethod("POST");     // request method, default GET
            urlconn.setUseCaches(false);    //Post can not user cache
            urlconn.setDoOutput(true);    //set output from urlconn
            urlconn.setDoInput(true);    //set input from urlconn
            
            //urlconn.set
            //urlconn.connect();

            //OutputStreamWriter out = new OutputStreamWriter(urlconn.getOutputStream(), inencoding);  
            //out.write(params);
            //out.flush();
            //out.close();    // output stream close,That's means need not to post data to this outputstream

            DataOutputStream out = new DataOutputStream(urlconn.getOutputStream());
            // The URL-encoded contend
            // 正文，正文内容其实跟get的URL中'?'后的参数字符串一致
            String content = params;
            
            // DataOutputStream.writeBytes将字符串中的16位的unicode字符以8位的字符形式写道流里面
            //byte[] output = Util.objectToBytes(params);
            out.writeBytes(content);
            out.flush();
            out.close(); // flush and close
            
            br = new BufferedReader(new InputStreamReader(urlconn.getInputStream(), inencoding));
            while((brLine = br.readLine())!=null)
                response =(new StringBuilder(String.valueOf(response))).append(brLine).toString();
        } catch (Exception e) {
        	log.error(e);
            
        } finally {
            try {
                if(br != null) {
                    br.close();
                }
            } catch (IOException e) {
                log.error("input stream close fail",e);
            }
            urlconn.disconnect();
        }
        return response;
    }

    public static void main(String[] args) {
    	StringBuffer sb=new StringBuffer();
		sb.append("ftpServer=192.168.0.200&");
		sb.append("ftpPort=21&");
		sb.append("user=ftp&");
		sb.append("password=ftp&");
		sb.append("passiveMode=true&");
		sb.append("encoding=UTF-8&");
		sb.append("remoteRoot=&");
		sb.append("filelist=/a");
        TransactionCenter tc = new TransactionCenter("UTF-8", "UTF-8");
        Map<String,String> paras = new HashMap();
        paras.put("username", "5555");
        paras.put("password", "pass");
        String response = tc.connect("j_username=admin&j_password=admin","http://utl002-pc:8888/WebServiceTest/servlet/j_spring_security_check");
        root rt = (root)Util.strSerialization(response, root.class);
		
        response = response.toUpperCase();
        String sessionid = rt.sessionId;
        
        System.out.println(sessionid);
    }
    
}
