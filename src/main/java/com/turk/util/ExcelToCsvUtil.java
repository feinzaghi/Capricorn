package com.turk.util;

import utele.Config.SystemConfig;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import jxl.Cell;
import jxl.Sheet;
import jxl.Workbook;
import org.apache.log4j.Logger;
import task.CollectObjInfo;
import task.RegatherObjInfo;
import utele.util.opencsv.CSVWriter;
import utele.util.*;

public class ExcelToCsvUtil
{
	private File source;
	private String keyId;
	private int taskId;
	private Timestamp dataTime;
	private Logger logger = LogMgr.getInstance().getSystemLogger();

	public ExcelToCsvUtil(File excelFile, CollectObjInfo taskInfo)
    	throws FileNotFoundException
    {
		int id = taskInfo.getTaskID();
		this.taskId = id;
		if ((taskInfo instanceof RegatherObjInfo))
			id = taskInfo.getKeyID() - 10000000;
		this.keyId = (taskInfo.getTaskID() + "-" + id);
		this.dataTime = taskInfo.getLastCollectTime();
		if (excelFile == null) {
			throw new FileNotFoundException(this.keyId + "-传入的文件路径为null");
		}
		if (!excelFile.exists()) {
			throw new FileNotFoundException(this.keyId + "-文件不存在:" + 
					excelFile.getAbsolutePath());
		}
		if (excelFile.isDirectory()) {
			throw new FileNotFoundException(this.keyId + "-传入的路径为目录，非文件:" + 
					excelFile.getAbsolutePath());
		}
		this.source = excelFile;
    }

	public ExcelToCsvUtil(String excelFile, CollectObjInfo taskInfo)
    	throws FileNotFoundException
    {
		this(new File(excelFile), taskInfo);
    }

	public List<String> toCsv()
    	throws Exception
	{
		return toCsv(null);
	}

	public List<String> toCsv(Character splitChar)
		throws Exception
    {
		List ret = new ArrayList();
		File dir = new File(SystemConfig.getInstance().getCurrentPath() + 
				File.separator + this.taskId + File.separator + 
				Util.getDateString_yyyyMMddHH(this.dataTime) + File.separator + 
				this.source.getName() + File.separator);
		if ((!dir.exists()) && 
				(!dir.mkdirs())) {
			throw new Exception(this.keyId + "-创建文件夹失败:" + dir.getAbsolutePath());
		}

		Workbook wb = Workbook.getWorkbook(this.source);
		this.logger.debug(this.keyId + "-开始将EXCEL文件转换为CSV文件:" + this.source.getAbsolutePath());
		Sheet[] sheets = wb.getSheets();
		for (Sheet sheet : sheets) 
		{
			File csvFile = new File(dir, sheet.getName() + ".csv");
			int columnsCount = sheet.getRow(0).length;
			int rowCount = sheet.getRows();
			String[] cols = new String[columnsCount];
			PrintWriter writer = new PrintWriter(csvFile);
			CSVWriter csvWriter = new CSVWriter(writer, splitChar == null ? ',' : 
				splitChar.charValue());
			for (int i = 0; i < rowCount; i++) 
			{
				Cell[] cells = sheet.getRow(i);
				for (int j = 0; j < columnsCount; j++) 
				{
					String content = j > cells.length - 1 ? "" : cells[j].getContents();
					content = content == null ? "" : content.replace('\r', ' ').replace('\n', ' ');
					cols[j] = content;
				}
				csvWriter.writeNext(cols);
				csvWriter.flush();
			}
			csvWriter.close();
			writer.close();
			ret.add(csvFile.getAbsolutePath());
		}

		this.logger.debug(this.keyId + "-CSV文件已转换完成,所在目录:" + dir.getAbsolutePath());
		return ret;
    }

	public static void main(String[] args) throws Exception {
		CollectObjInfo c = new CollectObjInfo(998);
		c.setLastCollectTime(
				new Timestamp(Util.getDate1("2010-10-15 00:00:00").getTime()));
		ExcelToCsvUtil u = new ExcelToCsvUtil(
				"D:\\20110107\\345-345-20110106-v3.17.310.xls", c);
		List<String> list = u.toCsv();
		for (String s : list)
			System.out.println(s);
	}
}