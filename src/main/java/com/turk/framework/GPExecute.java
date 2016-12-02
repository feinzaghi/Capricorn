package com.turk.framework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;

import org.apache.log4j.Logger;
import com.turk.db.GPCommonDB;

public class GPExecute implements Runnable{

	private Thread thread = new Thread(this, toString());
	private String _strSql = "";
	private static Logger log = Logger.getLogger(GPExecute.class);
	
	public void Execute(String strSql)
	{
		_strSql = strSql;
		this.thread.start();
	}
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		try
		{
			//log.debug("Starting getConnection...");
			conn = GPCommonDB.getConnection();
			//log.debug("GetConnection done...");
			if (conn == null)
			{
				log.error("��������ж�ȡ��Ϣʧ��,ԭ��:�޷���ȡ���ݿ�����.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = _strSql;
			//��ȡԤ�������
			Date start = new Date();
			int count = 0;
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		count++;
	    	}
	    	
	    	Date end = new Date();
             long  mint=(end.getTime()-start.getTime())/(1000);   
             log.debug("GPTEST-" + this.thread.getName() + " Finish: cost:"+ mint +"(s) Count:" + count + "");
	    	
	    	
	    	
		}catch (Exception e) {
				
				log.error("GPTEST:" + e.getMessage() ,e);
		}
		finally
		{
			GPCommonDB.close(rs, pstmt, conn);
		}
	}

}
