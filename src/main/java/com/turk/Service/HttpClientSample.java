package com.turk.Service;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.log4j.Logger;

import com.turk.util.LogMgr;

public class HttpClientSample {
	private final HttpClient	httpclient	= new DefaultHttpClient();
	protected static Logger log = LogMgr.getInstance().getSystemLogger();
	
	public String doPost(String url, Map<String, String> paraMap) {
		String Return = "";
		HttpPost httppost = new HttpPost(url);
		List<NameValuePair> params = new ArrayList<NameValuePair>();

		for (String key : paraMap.keySet()) {
			params.add(new BasicNameValuePair(key, paraMap.get(key)));
		}

		try {
			httppost.setEntity(new UrlEncodedFormEntity(params, HTTP.UTF_8));

			//System.out.println("\n\n开始执行POST>>>" + url);
			log.debug("\n\n开始执行POST>>>" + url);
			HttpResponse response = this.httpclient.execute(httppost);

			//System.out.println("HTTP POST状态：" + response.getStatusLine());
			log.debug("HTTP POST状态：" + response.getStatusLine());
			//System.out.println("----------------------------------------");
			Header[] headers = response.getAllHeaders();
			for (Header header : headers) {
				log.debug(header.getName() + ":" + header.getValue());
			}
			//System.out.println("----------------------------------------");

			HttpEntity entity = response.getEntity();

			if (entity != null) {
				InputStream instream = entity.getContent();
				try {

					InputStreamReader reader = new InputStreamReader(instream, "UTF-8");
					BufferedReader br = new BufferedReader(reader);

					String path = null;
					StringBuffer sb = new StringBuffer();
					while ((path = br.readLine()) != null) {
						log.debug(path);
						sb.append(path);
					}
					Return = sb.toString();
				} catch (RuntimeException ex) {
					httppost.abort();
					throw ex;
				} finally {
					try {
						instream.close();
					} catch (Exception ignore) {
					}
				}
			}

			/**
			 * 注意：
			 * 状态码 对应HttpServletResponse的常量 详细描述
			 * 301 SC_MOVED_PERMANENTLY 页面已经永久移到另外一个新地址
			 * 302 SC_MOVED_TEMPORARILY 页面暂时移动到另外一个新的地址
			 * 303 SC_SEE_OTHER 客户端请求的地址必须通过另外的URL来访问
			 * 307 SC_TEMPORARY_REDIRECT 同SC_MOVED_TEMPORARILY
			 */

			int statuscode = response.getStatusLine().getStatusCode();

			if ((statuscode == HttpStatus.SC_MOVED_TEMPORARILY)
					|| (statuscode == HttpStatus.SC_MOVED_PERMANENTLY)
					|| (statuscode == HttpStatus.SC_SEE_OTHER)
					|| (statuscode == HttpStatus.SC_TEMPORARY_REDIRECT)) {
				// 读取新的URL地址
				
				Header[] urlHeaders = response.getHeaders("Location");
				if (urlHeaders != null && urlHeaders.length > 0) {
					String jumpUrl = urlHeaders[0].getValue();
					log.debug("开始跳转>>>" + jumpUrl);
					Return = this.doGet(jumpUrl);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return Return;
	}

	public String doGet(String url) {
		HttpGet httpget = new HttpGet(url);
		String result = "";
		try {

			log.debug("\n\n开始执行GET>>>" + url);
			HttpResponse response = this.httpclient.execute(httpget);

			log.debug("HTTP GET状态：" + response.getStatusLine());

			//System.out.println("----------------------------------------");
			Header[] headers = response.getAllHeaders();
			for (Header header : headers) {
				System.out.println(header.getName() + ":" + header.getValue());
			}
			//System.out.println("----------------------------------------");

			HttpEntity entity = response.getEntity();
			InputStream instream = entity.getContent();
			InputStreamReader reader = new InputStreamReader(instream, "UTF-8");
			BufferedReader br = new BufferedReader(reader);
			String path = null;
			StringBuffer sb = new StringBuffer();
			while ((path = br.readLine()) != null) {
				log.info(path);
				sb.append(path);
			}

			br.close();
			instream.close();
			result = sb.toString();
			//System.out.println("----------------------------------------");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	public void doLogin() {
		Map<String, String> paraMap = new HashMap<String, String>();
		paraMap.put("j_username", "admin");
		paraMap.put("j_password", "admin");

		this.doPost("http://192.168.1.11:8080/OpenPlatform/j_spring_security_check", paraMap);

	}

	public void doLogout() {
		this.doPost("http://192.168.1.11:8080/OpenPlatform/j_spring_security_logout",
			new HashMap<String, String>());
	}

	public static void main(String[] args) {

		HttpClientSample hcs = new HttpClientSample();
		hcs.doLogin();


		Map<String, String> loaderPara = new HashMap<String, String>();
		loaderPara.put("ftpServer", "192.168.1.29");
		loaderPara.put("ftpPort", "21");
		loaderPara.put("user", "ftpuser");
		loaderPara.put("password", "ftpuser");
		loaderPara.put("passiveMode", "true");
		loaderPara.put("encoding", "UTF-8");
		loaderPara.put("remoteRoot", "./");
		loaderPara.put("filelist",
//			"./PSMM/gzbsc7/psmm_hw_20110621090000.txt\r\n./PSMM/gzbsc7/psmm_hw_20110621100000.txt");
			"");
		hcs.doPost("http://192.168.1.11:8080/OpenPlatform/loader", loaderPara);

 		Map<String, String> queryPara = new HashMap<String, String>();
		queryPara.put("querySql", "select count(*) from DT_20;");
		hcs.doPost("http://192.168.1.11:8080/OpenPlatform/query", queryPara);

		hcs.doLogout();
		// hcs.doGet("http://222.128.6.217:8080/OpenPlatform/query");
	}
}
