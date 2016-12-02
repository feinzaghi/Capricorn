package com.turk.dataimport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.config.SystemConfig;
import com.turk.util.CommonDB;
import com.turk.util.LogMgr;
import com.turk.util.Util;

public class BCPImport implements IOutput{

	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	public BCPImport()
	{
		
	}
	
	//ִ���ļ����
	public void ExcuteImport(int KeyID,String cltFileName,
			ColumnObject[] columns,String tableName,String fileName,String timeString,String strSplit)
	{
		try
		{
			//�����ļ���
			BufferedWriter bw = new BufferedWriter(
					new FileWriter(cltFileName, false));

	
			if ((Util.isSqlServer()))
			{
				//Sql Server
				//Map columns = _columns;
				bw.write("10.0\r\n");
				int nField = columns.length;
				bw.write(String.valueOf(nField) + "\r\n");

				for (int i = 1; i <= nField; i++)
				{
					bw.write(i + "\tSQLCHAR\t0\t128\t");
					if (i < nField)
					{
						bw.write("\""+strSplit+"\"");
					}
					else
					{
						bw.write("\""+strSplit+"\n\"");
					}

					String strField = columns[i-1].ColumnName;
					bw.write("\t" + i + "\t" + strField + "\tChinese_PRC_CI_AS \r\n");
				}
				bw.flush();
				bw.close();
				
				RunSqlLoad(tableName, fileName, cltFileName, timeString);
			}

			if (SystemConfig.getInstance().isDeleteLog())
			{
				File ctlfile = new File(cltFileName);
				if (ctlfile.exists()) {
					ctlfile.delete();
				}

				String strTxt = fileName;
				File txtfile = new File(strTxt);
				if (txtfile.exists())
				{
					if (txtfile.delete())
					{
						this.log.debug("·���������ļ�" + ": " + strTxt + 
						"ɾ���ɹ�....");
					}
					else
					{
						this.log.debug("·���������ļ�" + ": " + strTxt + 
						"ɾ��ʧ��");
					}
				}
				else
				{
					this.log.debug("·���������ļ�" + ": " + strTxt + 
					"δ�ҵ����޷�ɾ��");
				}
			}
		}
		catch (Exception e)
		{
			this.log.error("BuildBCP", e);
		}
	}
	
	
	/**
	 * SQLLoad���
	 * @param strTable
	 * @param txtFile
	 * @param ctlFile
	 */
	private void RunSqlLoad(String strTable, String txtFile,String ctlFile,String timeString)
	{
		try
		{
			//��ǰ��������Ŀ¼
			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strUrl = SystemConfig.getInstance().getDbUrl();
			
			//���ݿ�����
			String strBase = CommonDB.getConnection().getCatalog();
			//���ݿ��½�û���
			String strUserName = SystemConfig.getInstance().getDbUserName();
			//���ݿ��½����
			String strPassword = SystemConfig.getInstance().getDbPassword();
			//���ݿ�IP ����������
			String strService = SystemConfig.getInstance().getDbService();
			//BCP������־
			String strLog = strCurrentPath + File.separatorChar + "ldrlog" + 
			File.separatorChar + timeString + ".log";
			String strDataFile = txtFile;
			
			//���ݸ�ʽ�ļ�
			String strFmt = ctlFile;

			/*bcp UTL.t_DTCDMA_ActiveSet in "D:\\Project\\JAVA\\UTL_Collect\\data\0_100001_20110302000000_0.txt" 
			 *-U utl -P utl2011 -S 192.168.0.10 -c -t ";" -r -e D:\\Project\\JAVA\\UTL_Collect\\data\ldrlog\0_100001_20110302000000_0.log*/
			//�ų���һ��ͷ�ַ���
			String cmd = String.format("bcp %s.%s in \"%s\" -U %s -P %s -S %s -t; -r\\n -e %s -f \"%s\" -F 1"
					, new Object[] { strUserName, strTable, strDataFile, strUserName, strPassword, strService, strLog,strFmt });

			log.debug("ִ��BCP ��⣺" + cmd);
			
			StringBuffer cmdout = new StringBuffer(); 
			Process ldr = Runtime.getRuntime().exec(cmd);
			//ִ�еȴ������ܻ�����ִ����������¹���
			//ldr.waitFor();
		    InputStream fis = ldr.getInputStream(); 
            BufferedReader br = new BufferedReader(new InputStreamReader(fis)); 
            String line = null; 
          
            while ((line = br.readLine()) != null) { 
                cmdout.append(line); 
            }
            log.debug("ִ��ϵͳ�����Ľ��Ϊ��\n" + cmdout.toString()); 
		}
		catch (Exception e)
		{
			this.log.error("BCP Error!", e);
		}
	}

	@Override
	public void ExcuteImport(int KeyID,String DBServer, String userid, String password,
			String cltFileName, ColumnObject[] columns, String tableName,
			String fileName, String timeString,String strSplit) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setExecuteImmediate(boolean ExecuteImmediate) {
		// TODO Auto-generated method stub
		
	}
}
