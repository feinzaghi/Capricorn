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
	
	//执行文件入库
	public void ExcuteImport(int KeyID,String cltFileName,
			ColumnObject[] columns,String tableName,String fileName,String timeString,String strSplit)
	{
		try
		{
			//控制文件流
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
						this.log.debug("路测汇总入库文件" + ": " + strTxt + 
						"删除成功....");
					}
					else
					{
						this.log.debug("路测汇总入库文件" + ": " + strTxt + 
						"删除失败");
					}
				}
				else
				{
					this.log.debug("路测汇总入库文件" + ": " + strTxt + 
					"未找到，无法删除");
				}
			}
		}
		catch (Exception e)
		{
			this.log.error("BuildBCP", e);
		}
	}
	
	
	/**
	 * SQLLoad入库
	 * @param strTable
	 * @param txtFile
	 * @param ctlFile
	 */
	private void RunSqlLoad(String strTable, String txtFile,String ctlFile,String timeString)
	{
		try
		{
			//当前程序运行目录
			String strCurrentPath = SystemConfig.getInstance().getCurrentPath();
			String strUrl = SystemConfig.getInstance().getDbUrl();
			
			//数据库名称
			String strBase = CommonDB.getConnection().getCatalog();
			//数据库登陆用户名
			String strUserName = SystemConfig.getInstance().getDbUserName();
			//数据库登陆密码
			String strPassword = SystemConfig.getInstance().getDbPassword();
			//数据库IP 服务器名称
			String strService = SystemConfig.getInstance().getDbService();
			//BCP导入日志
			String strLog = strCurrentPath + File.separatorChar + "ldrlog" + 
			File.separatorChar + timeString + ".log";
			String strDataFile = txtFile;
			
			//数据格式文件
			String strFmt = ctlFile;

			/*bcp UTL.t_DTCDMA_ActiveSet in "D:\\Project\\JAVA\\UTL_Collect\\data\0_100001_20110302000000_0.txt" 
			 *-U utl -P utl2011 -S 192.168.0.10 -c -t ";" -r -e D:\\Project\\JAVA\\UTL_Collect\\data\ldrlog\0_100001_20110302000000_0.log*/
			//排除第一行头字符串
			String cmd = String.format("bcp %s.%s in \"%s\" -U %s -P %s -S %s -t; -r\\n -e %s -f \"%s\" -F 1"
					, new Object[] { strUserName, strTable, strDataFile, strUserName, strPassword, strService, strLog,strFmt });

			log.debug("执行BCP 入库：" + cmd);
			
			StringBuffer cmdout = new StringBuffer(); 
			Process ldr = Runtime.getRuntime().exec(cmd);
			//执行等待，可能会由于执行命令错误导致挂死
			//ldr.waitFor();
		    InputStream fis = ldr.getInputStream(); 
            BufferedReader br = new BufferedReader(new InputStreamReader(fis)); 
            String line = null; 
          
            while ((line = br.readLine()) != null) { 
                cmdout.append(line); 
            }
            log.debug("执行系统命令后的结果为：\n" + cmdout.toString()); 
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
