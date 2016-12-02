package com.turk.util;

import java.io.File;
import java.io.IOException;

import com.turk.config.SystemConfig;

public class GpTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String localFile="E:\\Workspaces\\Capricorn\\data\\DTCALU\\DT_Area_1X_20_20110623.txt";
		String fileName=localFile.substring((localFile.lastIndexOf("\\")+1),localFile.length());
		String ackLocalFile="";
		// TODO Auto-generated method stub
		if(SystemConfig.getInstance().isGp()){
    		FTPInfo gpInfo=new FTPInfo();
    		gpInfo.setIP(SystemConfig.getInstance().getGpIp());
    		gpInfo.setPort(Integer.parseInt(SystemConfig.getInstance().getGpPort()));
    		gpInfo.setUser(SystemConfig.getInstance().getGpUser());
    		gpInfo.setPwd(SystemConfig.getInstance().getGpPwd());
    		gpInfo.setEncode(SystemConfig.getInstance().getGpEncoding());
	    	//log.info("�ļ�" + fileName + "��ʼ�ϴ�FTP...");
    		
    		String remotePath = SystemConfig.getInstance().getGpRemoteRootDT();
	 		FTPToolCommon ftpobj = new FTPToolCommon(gpInfo);
	 		ftpobj.login(2000, 3);
	 		int code = ftpobj.uploadFile(localFile, remotePath);
			switch(code)
			{
			   	case 100://�ɹ�
			   		//log.info("�ļ�" + fileName + "�ϴ��ɹ�");
			    	break;
			    case 400:
			    		//�쳣
			    	//log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
			    	break;
			    case 401:
			    		//��������ʧ��
			    	//log.warn("�ļ�" + fileName + "�ϴ�ʧ��");
			    	break;
			 }
			
		    ackLocalFile=localFile.substring(0,localFile.lastIndexOf('.'))+".ack";
			File file=new File(ackLocalFile); 
			
			if(!file.exists())
			{
				try {
					file.createNewFile();
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			FTPToolCommon ftpobj1 = new FTPToolCommon(gpInfo);
	 		ftpobj1.login(2000, 3);
			int ackCode=ftpobj1.uploadFile(ackLocalFile, remotePath);
			switch(ackCode)
			{
			   	case 100://�ɹ�
			   		//log.info("�ļ�" + ackLocalFile + "�ϴ��ɹ�");
			    	break;
			    case 400:
			    		//�쳣
			    	//log.warn("�ļ�" + ackLocalFile + "�ϴ�ʧ��");
			    	break;
			    case 401:
			    		//��������ʧ��
			    	//log.warn("�ļ�" + ackLocalFile + "�ϴ�ʧ��");
			    	break;
			 }
			
			ftpobj.disconnect();
			
			
			
			//NoticeHttp(gpInfo,remotePath,fileName);
			 
			//log.info("�ļ�"+fileName+"�ϴ��ɹ�");
    	}
	}

}
