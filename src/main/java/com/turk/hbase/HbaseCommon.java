package com.turk.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseCommon {
	
	static Configuration cfg = null;
	static {
		
		Configuration HBASE_CONFIG = HBaseConfiguration.create();
		
		HBASE_CONFIG.set("hbase.zookeeper.quorum", "hive-2");  //ǧ�����������
		//HBASE_CONFIG.set("hbase.zookeeper.property.clientPort","2181");
		//HBASE_CONFIG.set("fs.defaultFS","hdfs://hive-2:8020");
		//HBASE_CONFIG.set("zookeeper.znode.parent","/user/hbase");
		//0.96 ��
		//System.setProperty("hadoop.home.dir", "C:\\cygwin\\usr\\hadoop");
		cfg = HBaseConfiguration.create(HBASE_CONFIG);
	}
	
	/**
	 * 
	 * @param tablename
	 * @throws Exception
	 */
	@SuppressWarnings("resource")
	public static ArrayList<HashMap<String,String>> getAllData(String tablename) throws Exception{
		
		ArrayList<HashMap<String,String>> result = new ArrayList<HashMap<String, String>>();
		
		HBaseAdmin admin =new HBaseAdmin(cfg);  
        if (admin.tableExists(tablename)) {  
            System.out.println("���Ѿ����ڣ�");  
        }
		HTable table = new HTable(cfg,tablename);
		Scan s = new Scan();
		ResultScanner ss = table.getScanner(s);
		
		for (Result r : ss)
		{//��
			HashMap<String,String> row = new HashMap<String, String>();
			for(Cell c : r.listCells())
			{//��
				
				String family = Bytes.toString(CellUtil.cloneFamily(c));//�д�
				String column = Bytes.toString(CellUtil.cloneQualifier(c));
				String value = Bytes.toString(CellUtil.cloneValue(c));//ֵ
				String key = family + ":" + column;
				row.put(key, value);
			}
		}
		
		return result;
	}
	
	
	public static HashMap<String,String> getDataByKey(String tablename,String rowkey) throws Exception
	{
		HTable table = new HTable(cfg,tablename);
		HashMap<String,String> result = new HashMap<String, String>();
		
		try {
			
		   Get get = new Get(rowkey.getBytes()); //����������ѯ
		   Result r = table.get(get);
		 
		   for(Cell c : r.listCells()){
		    //ʱ���ת�������ڸ�ʽ
		    String timestampFormat
		    	= new SimpleDateFormat("yyyy-MM-dd HH:MM:ss")
		    .format(new Date(c.getTimestamp()));
		       //System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
		    String family = Bytes.toString(CellUtil.cloneFamily(c));//�д�
			String column = Bytes.toString(CellUtil.cloneQualifier(c));
			String value = Bytes.toString(CellUtil.cloneValue(c));//ֵ
			String key = family + ":" + column;
			result.put(key, value);
		 
		   }
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		  return result;
	}
	
	/**
	 * ����rowkey�ķ�Χ��ѯ
	 * @param tablename
	 * @param startkey
	 * @param endkey
	 * @throws Exception
	 */
	public static void getData(String tablename,String column,String startkey,String endkey) throws Exception
	{
		HTable table = new HTable(cfg,tablename);
		int rownum = 0;
		try {
			
		   Scan sc = new Scan(startkey.getBytes(), endkey.getBytes());
		   sc.addColumn(Bytes.toBytes("info"),Bytes.toBytes(column));
		   ResultScanner ss = table.getScanner(sc);
		   
		   
		   for (Result r : ss)
			{//��
			   
			   
			   rownum++;
			   for(Cell c : r.listCells()){
			    //ʱ���ת�������ڸ�ʽ
			    String timestampFormat
			    	= new SimpleDateFormat("yyyy-MM-dd HH:MM:ss")
			    .format(new Date(c.getTimestamp()));
			       //System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
			    System.out.println("\nKeyValue: "+c);
			    //System.out.println("key: "+kv.getKeyString());
			    
			    System.out.println("family=>"+Bytes.toString(CellUtil.cloneFamily(c))
				          +"  value=>"+Bytes.toString(CellUtil.cloneValue(c))
				    +"  qualifer=>"+Bytes.toString(CellUtil.cloneQualifier(c))		    
				    +"  timestamp=>"+timestampFormat);
			 
			   }
			}
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		  System.out.println("end===========row:" + rownum);
	}
	
	
	 public static void QueryByCondition2(String tablename,String column1,String value1) { 
	        try { 
	        	HTable table = new HTable(cfg,tablename);
	            Filter filter = new SingleColumnValueFilter(Bytes.toBytes(column1), null, CompareOp.EQUAL, Bytes 
	                    .toBytes(value1)); // ����column1��ֵΪaaaʱ���в�ѯ 
	            Scan s = new Scan(); 
	            s.setFilter(filter); 
	            ResultScanner rs = table.getScanner(s); 
	            for (Result r : rs) { 
	                System.out.println("����rowkey:" + new String(r.getRow())); 
	                for (Cell c : r.listCells()) { 
	                    System.out.println("�У�" 
	                + Bytes.toString(CellUtil.cloneQualifier(c)) 
	                +" ====ֵ:" + Bytes.toString(CellUtil.cloneValue(c))); 
	                } 
	            } 
	        } catch (Exception e) { 

	            e.printStackTrace(); 

	        } 

	 

	    } 

	 

	     

	    public static void QueryByCondition3(String tablename,
	    		String column1,String value1,
	    		String column2,String value2,
	    		String column3,String value3) { 

	 

	        try { 

	        	HTable table = new HTable(cfg,tablename);

	            List<Filter> filters = new ArrayList<Filter>(); 

	 

	            Filter filter1 = new SingleColumnValueFilter(Bytes 

	                    .toBytes(column1), null, CompareOp.EQUAL, Bytes 

	                    .toBytes(value1)); 

	            filters.add(filter1); 

	 

	            Filter filter2 = new SingleColumnValueFilter(Bytes 

	                    .toBytes(column2), null, CompareOp.EQUAL, Bytes 

	                    .toBytes(value2)); 

	            filters.add(filter2); 

	 

	            Filter filter3 = new SingleColumnValueFilter(Bytes 

	                    .toBytes(column3), null, CompareOp.EQUAL, Bytes 

	                    .toBytes(value3)); 

	            filters.add(filter3); 

	 

	            FilterList filterList1 = new FilterList(filters); 

	 

	            Scan scan = new Scan(); 

	            scan.setFilter(filterList1); 

	            ResultScanner rs = table.getScanner(scan); 

	            for (Result rr : rs) { 

	                System.out.println("����rowkey: " + new String(rr.getRow())); 

	                for (Cell c : rr.listCells()) { 

	                    System.out.println("�У� "+ Bytes.toString(CellUtil.cloneQualifier(c))

	                            +"====ֵ:" + Bytes.toString(CellUtil.cloneValue(c))); 
	                } 

	            } 

	            rs.close(); 

	 

	        } catch (Exception e) { 

	            e.printStackTrace(); 

	        } 

	 

	    } 
	
	public static void main(String[] args){
		try{
			String tablename="cdr_hw_1x";
			//HBaseTestCase.getAllData(tablename);
			//HBaseTestCase.getDataByKey(tablename,"2014031011300004_turk");
			
			HbaseCommon.getData(tablename,"BSC","2013-01-01","2013-01-02");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
