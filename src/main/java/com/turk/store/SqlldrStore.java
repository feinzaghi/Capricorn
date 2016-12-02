package com.turk.store;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import com.turk.config.SystemConfig;
import com.turk.exception.StoreException;
import com.turk.task.CollectObjInfo;
import com.turk.util.ExternalCmd;
import com.turk.util.SqlldrResult;
import com.turk.util.Util;
import com.turk.util.loganalyzer.SqlLdrLogAnalyzer;

public class SqlldrStore extends AbstractStore<SqlldrStoreParam>
{
	private FileWriter fWriter;
	private List<String> buffer = new ArrayList<String>(200);
	private static final int MAX_SIZE = 200;
	private SqlldrInfo info;

	public SqlldrStore()
	{
	}

	public SqlldrStore(SqlldrStoreParam param)
	{
		super(param);
	}

	public void open() throws StoreException
	{
		SqlldrStoreParam param = (SqlldrStoreParam)getParam();
		if (param == null) {
			throw new StoreException("ȱ��StoreParam����");
		}
		this.info = fillSqlldrInfo();

		makeFile_Ctl(this.info, param);
    
		makeFile_Txt_Head(this.info, param);
	}

	public void flush() throws StoreException {
		writeDataToTxtFile();
	}

	public void commit()
    	throws StoreException
    {
		runSqlldr();
    }

	public void write(String data)
		throws StoreException
    {
		this.buffer.add(data);
		if (this.buffer.size() >= 200)
			writeDataToTxtFile();
    }

	public void close()
  	{
		if (this.fWriter != null)
		{
			try {
				this.fWriter.flush();
				this.fWriter.close();
			}
			catch (IOException localIOException) {
			}
		}
		this.buffer.clear();
		this.info = null;
  	}

	private SqlldrInfo fillSqlldrInfo() 
	{
		SqlldrInfo info = new SqlldrInfo();

		String tbName = ((SqlldrStoreParam)getParam()).getTable().getName();
		int taskID = getTaskID();
		Timestamp dataTime = getDataTime();
		String strDateTime = Util.getDateString_yyyyMMddHHmmss(dataTime);

		String fileNamePrex = taskID + "_" + tbName + "_" + strDateTime + "_" + 
			((SqlldrStoreParam)getParam()).getTable().getId() + (
					getFlag() == null ? "" : new StringBuilder("_").append(getFlag()).toString());

		String folder = SystemConfig.getInstance().getCurrentPath() + 
			File.separator + "ldrlog" + File.separator;
		File folderF = new File(folder);
		if ((!folderF.exists()) || (!folderF.isDirectory())) {
			folderF.mkdir();
		}

		Calendar calendar = Calendar.getInstance();
		int month = calendar.get(2) + 1;
		int year = calendar.get(1);
		String yearFolder = folder + year + "-" + month + File.separator;
		File yearFolderF = new File(yearFolder);
		if ((!yearFolderF.exists()) || (!yearFolderF.isDirectory())) {
			yearFolderF.mkdir();
		}
		String name = yearFolder + fileNamePrex;
		info.ctlFile = (name + ".ctl");
		info.logFile = (name + ".log");
		info.badFile = (name + ".bad");
		info.txtFile = (name + ".txt");
		info.tbName = tbName;

		return info;
	}

	private void makeFile_Ctl(SqlldrInfo info, SqlldrStoreParam param)
    	throws StoreException
    {
		File f = new File(info.ctlFile);
		PrintWriter pw = null;
		try {
			String split = param.getTable().getSplitSign();

			String columnNameList = param.getTable().listColumnNamesWithType(
				",");

			pw = new PrintWriter(f);
			pw.println("load data");
			pw.println("CHARACTERSET " + 
					SystemConfig.getInstance().getSqlldrCharset());
			pw.println("infile '" + info.txtFile + "' append into table " + 
							info.tbName);
			pw.println("FIELDS TERMINATED BY \"" + split + "\"");
			pw.println("TRAILING NULLCOLS");
			pw.print("(" + columnNameList);
			pw.print("DEVICEID,COLLECTTIME Date 'YYYY-MM-DD HH24:MI:SS',STAMPTIME Date 'YYYY-MM-DD HH24:MI:SS'");
			pw.print(")");
			pw.flush();
			pw.close();
		} catch (Exception e) {
			throw new StoreException("�ɼ��ļ�(" + info.ctlFile + ")д��ʧ��", e);
		} finally {
			if (pw != null)
				pw.close();
		}
    }

	private void writeDataToTxtFile()
    	throws StoreException
	{
		try
		{
			String split = ((SqlldrStoreParam)getParam()).getTable().getSplitSign();
			if (this.fWriter == null) {
				this.fWriter = new FileWriter(new File(this.info.txtFile), true);
			}

			String nowStr = Util.getDateString(new Date());
			String sysFieldValue = getDeviceID() + split + nowStr + split + 
			Util.getDateString(getDataTime());

			for (String str : this.buffer) {
				this.fWriter.write(str + sysFieldValue + "\n");
			}
			this.fWriter.flush();
		} catch (Exception e) {
			if (this.fWriter != null)
				try {
					this.fWriter.close();
        } catch (IOException localIOException) {
        }
        throw new StoreException("����д���ļ�(" + this.info.txtFile + ")ʱ�쳣.", e);
		} finally {
			this.buffer.clear();
		}
	}

	private void makeFile_Txt_Head(SqlldrInfo info, SqlldrStoreParam param)
		throws StoreException
    {
		try
		{
			String split = param.getTable().getSplitSign();
			String strHead = param.getTable().listColumnNames(split) + "DEVICEID" + 
			split + "COLLECTTIME" + split + "STAMPTIME\n";
			if (this.fWriter == null) {
				this.fWriter = new FileWriter(new File(info.txtFile), false);
			}
			this.fWriter.write(strHead);
			this.fWriter.flush();
		} catch (Exception e) {
			if (this.fWriter != null)
				try {
					this.fWriter.close();
				}
			catch (IOException localIOException) {
			}
			throw new StoreException("����txt�����ļ�(" + info.txtFile + ")��ͷʧ��", e);
		}
    }

	private void runSqlldr()
    	throws StoreException
    {
		CollectObjInfo collectInfo = getCollectInfo();
		int ret = -1;
		if (collectInfo != null) {
			Date now = new Date();
			collectInfo.setSqlldrTime(new Timestamp(now.getTime()));
		}

		String strOracleBase = SystemConfig.getInstance().getDbService();
		String strOracleUserName = SystemConfig.getInstance().getDbUserName();
    	String strOraclePassword = SystemConfig.getInstance().getDbPassword();
    	String cp = collectInfo.getCollectPath();
    	int skip = 1;
    	if ((Util.isNotNull(cp)) && (cp.toLowerCase().contains(".xls"))) {
    		skip = 2;
    	}
    	String cmd = 
    		String.format(
    				"sqlldr userid=%s/%s@%s skip=%s control=%s bad=%s log=%s errors=9999999", new Object[] { 
    						strOracleUserName, strOraclePassword, strOracleBase, 
    						Integer.valueOf(skip), this.info.ctlFile, this.info.badFile, this.info.logFile });
    	ExternalCmd externalCmd = new ExternalCmd();
    	String key = String.format("[taskId-%s][%s]", new Object[] { Integer.valueOf(collectInfo.getTaskID()), 
    			Util.getDateString(collectInfo.getLastCollectTime()) });
    	log.debug(key + "��ǰִ�е�SQLLDR����Ϊ��" + cmd.replace(strOracleUserName, "*").replace(
    						strOraclePassword, "*"));
    	try
    	{
    		ret = externalCmd.execute(cmd);
    		log.debug(key + "sqlldr������Ϊ:" + ret);
    	} catch (Exception e) {
    		throw new StoreException(key + "ִ��sqlldr����ʧ��(" + cmd + ")", e);
    	}

    	SqlLdrLogAnalyzer analyzer = new SqlLdrLogAnalyzer();
    	try {
    		SqlldrResult result = analyzer.analysis(
    				new FileInputStream(this.info.logFile));
    		if (result == null) {
    			return;
    		}
    		log.debug(key + " SQLLDR��־�������: DeviceID=" + getDeviceID() + 
    				" ���ɹ�����=" + result.getLoadSuccCount() + " ����=" + 
    				result.getTableName() + " ����ʱ��=" + getDataTime() + 
    				" sqlldr��־=" + this.info.logFile);

    		dbLogger.log(getDeviceID(), result.getTableName(), 
    				getDataTime(), result.getLoadSuccCount(), 
    				getTaskID(),0);
    	} catch (Exception e) {
    		log.error(key + " sqlldr��־����ʧ�ܣ��ļ�����" + this.info.logFile + "��ԭ��: ", e);
    	}

    	if (SystemConfig.getInstance().isDeleteLog())
    		delLog(ret);
    }

	private void delLog(int ret)
	{
		File badFile = new File(this.info.badFile);
		if (((badFile.exists()) && (badFile.isFile())) || (ret != 0)) {
			return;
		}

		delFile(this.info.ctlFile);

		if (this.fWriter != null) {
			try {
				this.fWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		delFile(this.info.txtFile);

		delFile(this.info.logFile);
	}

	private void delFile(String fPath) {
		File f = new File(fPath);
		if (f.exists())
			f.delete();
	}
}