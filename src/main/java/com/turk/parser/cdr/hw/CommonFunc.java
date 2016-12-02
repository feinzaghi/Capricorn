package com.turk.parser.cdr.hw;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.turk.util.CommonDB;
import com.turk.util.LogMgr;

public class CommonFunc {

	private static Logger log = LogMgr.getInstance().getSystemLogger();
	
	public static Map<Integer,CITYINFO> 
		g_arCityInfo = new HashMap<Integer, CITYINFO>();
	
	public static void InitCityInfo()
	{
		Connection conn = null;
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		int nGridSquare = 100; 
		try
		{
			
			conn = CommonDB.getConnection();
			//log.debug("GetConnection done...");
			if (conn == null)
			{
				log.error("从任务表中读取信息失败,原因:无法获取数据库连接.");
				Thread.sleep(60*1000L);
				return;
			}
			
			String strSql = "";
			//获取预定义分组
			strSql = "select city_id,nvl(gridstartid,0) gridstartid,longitude_lb," +
					"nvl(longitude_rb,0) longitude_rb,latitude_lb,nvl(latitude_rb,0) latitude_rb,nvl(SID,0) SID " +
					"from cfg_city where latitude_rb is not null order by city_id";
			pstmt = conn.prepareStatement(strSql);
			rs = pstmt.executeQuery();
	    	while(rs.next())
	    	{
	    		CITYINFO info = new CITYINFO();
	    		info.nCityId = rs.getInt("city_id");
	    		info.uStartId = rs.getInt("gridstartid");
	    		info.dwMinLon = rs.getDouble("longitude_lb");
	    		info.dwMaxLon = rs.getDouble("longitude_rb");
	    		info.dwMinLat = rs.getDouble("latitude_lb");
	    		info.dwMaxLat = rs.getDouble("latitude_rb");
	    		info.nSystemID = rs.getInt("SID");
	    		
	    		g_arCityInfo.put(info.nCityId, info);
	    		//计算距离
				CMapLonLat mapLonLat = new CMapLonLat();
				TJWD JwdCenter = new TJWD(g_arCityInfo.get(info.nCityId).dwMinLon,g_arCityInfo.get(info.nCityId).dwMinLat);
				TJWD JwdTmp = new TJWD(g_arCityInfo.get(info.nCityId).dwMaxLon,g_arCityInfo.get(info.nCityId).dwMinLat);
				g_arCityInfo.get(info.nCityId).dwWidth = mapLonLat.distance(JwdCenter,JwdTmp);
				JwdTmp.SetPoint(g_arCityInfo.get(info.nCityId).dwMinLon,g_arCityInfo.get(info.nCityId).dwMaxLat);
				g_arCityInfo.get(info.nCityId).dwHeigh = mapLonLat.distance(JwdCenter,JwdTmp);
				
				/////////////////////////////
				//计算格子个数,GRIDM表示宽度,GRIDN表示高度
				g_arCityInfo.get(info.nCityId).nGridM = (int)g_arCityInfo.get(info.nCityId).dwWidth/nGridSquare;
				g_arCityInfo.get(info.nCityId).nGridN = (int)g_arCityInfo.get(info.nCityId).dwHeigh/nGridSquare+1;
				
				/////////////////////////////////////////
				//计算每个格子大概占有经纬度值
				g_arCityInfo.get(info.nCityId).dwLon_100 = (g_arCityInfo.get(info.nCityId).dwMaxLon 
						- g_arCityInfo.get(info.nCityId).dwMinLon)/g_arCityInfo.get(info.nCityId).nGridM;
				g_arCityInfo.get(info.nCityId).dwLat_100 = (g_arCityInfo.get(info.nCityId).dwMaxLat 
						- g_arCityInfo.get(info.nCityId).dwMinLat)/g_arCityInfo.get(info.nCityId).nGridN;
				
	    	}
		}catch (Exception e) {
				
				log.error("网格初始化类异常:" + e.getMessage() ,e);
		}
		finally
		{
			CommonDB.close(rs, pstmt, conn);
		}
	}
	
	
	 /**
	   * 将int转为低字节在前，高字节在后的byte数组
	   */
	public static byte[] toLH(int n) {
	    byte[] b = new byte[4];
	    b[0] = (byte) (n & 0xff);
	    b[1] = (byte) (n >> 8 & 0xff);
	    b[2] = (byte) (n >> 16 & 0xff);
	    b[3] = (byte) (n >> 24 & 0xff);
	    return b;
	  }
	  
	public static int bytesToInt(byte[] b)
	  {
		  return    b[0] & 0xff 
      | (b[1] & 0xff) << 8 
      | (b[2] & 0xff) << 16
      | (b[3] & 0xff) << 24;
	  }

	  /**
	   * 将float转为低字节在前，高字节在后的byte数组
	   */
	public static byte[] toLH(float f) {
	    return toLH(Float.floatToRawIntBits(f));
	  }
	
	
	/**
	* 通过byte数组取到short
	*
	* @param b
	* @param index
	*            第几位开始取
	* @return
	*/
	public static short getShort(byte[] b, int index) {
	      return (short) (((b[index + 1] << 8) | b[index + 0] & 0xff));
	}

	/** 
     * 将一个单字节的byte转换成32位的int 
     *  
     * @param b 
     *            byte 
     * @return convert result 
     */  
    public static int unsignedByteToInt(byte b) {  
        return (int) b & 0xFF;  
    }  
  
    /** 
     * 将一个单字节的Byte转换成十六进制的数 
     *  
     * @param b 
     *            byte 
     * @return convert result 
     */  
    public static String byteToHex(byte b) {  
        int i = b & 0xFF;  
        return Integer.toHexString(i);  
    }  
  
    /** 
     * 将一个4byte的数组转换成32位的int 
     *  
     * @param buf 
     *            bytes buffer 
     * @param byte[]中开始转换的位置 
     * @return convert result 
     */  
    public static long unsigned4BytesToInt(byte[] buf, int pos) {  
        int firstByte = 0;  
        int secondByte = 0;  
        int thirdByte = 0;  
        int fourthByte = 0;  
        int index = pos;  
        firstByte = (0x000000FF & ((int) buf[index]));  
        secondByte = (0x000000FF & ((int) buf[index + 1]));  
        thirdByte = (0x000000FF & ((int) buf[index + 2]));  
        fourthByte = (0x000000FF & ((int) buf[index + 3]));  
        index = index + 4;  
        return ((long) (firstByte << 24 | secondByte << 16 | thirdByte << 8 | fourthByte)) & 0xFFFFFFFFL;  
    }  
  
    /** 
     * 将16位的short转换成byte数组 
     *  
     * @param s 
     *            short 
     * @return byte[] 长度为2 
     * */  
    public static byte[] shortToByteArray(short s) {  
        byte[] targets = new byte[2];  
        for (int i = 0; i < 2; i++) {  
            int offset = (targets.length - 1 - i) * 8;  
            targets[i] = (byte) ((s >>> offset) & 0xff);  
        }  
        return targets;  
    }  
  
    /** 
     * 将32位整数转换成长度为4的byte数组 
     *  
     * @param s 
     *            int 
     * @return byte[] 
     * */  
    public static byte[] intToByteArray(int s) {  
        byte[] targets = new byte[2];  
        for (int i = 0; i < 4; i++) {  
            int offset = (targets.length - 1 - i) * 8;  
            targets[i] = (byte) ((s >>> offset) & 0xff);  
        }  
        return targets;  
    }  
  
    /** 
     * long to byte[] 
     *  
     * @param s 
     *            long 
     * @return byte[] 
     * */  
    public static byte[] longToByteArray(long s) {  
        byte[] targets = new byte[2];  
        for (int i = 0; i < 8; i++) {  
            int offset = (targets.length - 1 - i) * 8;  
            targets[i] = (byte) ((s >>> offset) & 0xff);  
        }  
        return targets;  
    }  
  
    /**32位int转byte[]*/  
    public static byte[] int2byte(int res) {  
        byte[] targets = new byte[4];  
        targets[0] = (byte) (res & 0xff);// 最低位  
        targets[1] = (byte) ((res >> 8) & 0xff);// 次低位  
        targets[2] = (byte) ((res >> 16) & 0xff);// 次高位  
        targets[3] = (byte) (res >>> 24);// 最高位,无符号右移。  
        return targets;  
    }  
  
    /** 
     * 将长度为2的byte数组转换为16位int 
     *  
     * @param res 
     *            byte[] 
     * @return int 
     * */  
    public static int byte2int(byte[] res) {  
        // res = InversionByte(res);  
        // 一个byte数据左移24位变成0x??000000，再右移8位变成0x00??0000  
    	
        int targets = (res[0] & 0xff) | ((res[1] << 8) & 0xff00); // | 表示安位或  
        return targets;  
    }  
    
    public static int bytesToIntNew(byte[] intByte) {
    	int fromByte = 0;
    	for (int i = 0; i < 2; i++)
    	{
    		int n = (intByte[i] < 0 ? (int)intByte[i] + 256 : (int)intByte[i]) << (8 * i);
    		//System.out.println(n);
    		fromByte += n;
    	}
    	return fromByte;
 }
    
    public static int byteToint(byte[] res) {  
	    ByteArrayInputStream in = new ByteArrayInputStream(res);  
	    int result = in.read();  
	    return result;
    }
    
    /* 把16进制字符串转换成字节数组
    * @param hex
    * @return
    */
    public static byte[] hexStringToByte(String hex) {
	    int len = (hex.length() / 2);
	    byte[] result = new byte[len];
	    char[] achar = hex.toCharArray();
	    for (int i = 0; i < len; i++) {
	     int pos = i * 2;
	     result[i] = (byte) (toByte(achar[pos]) << 4 | toByte(achar[pos + 1]));
	    }
	    return result;
    }
    
    private static byte toByte(char c) {
	    byte b = (byte) "0123456789ABCDEF".indexOf(c);
	    return b;
    }
    /** *//**
    * 把字节数组转换成16进制字符串
    * @param bArray
    * @return
    */
    public static final String bytesToHexString(byte[] bArray) {
	    StringBuffer sb = new StringBuffer(bArray.length);
	    String sTemp;
	    for (int i = 0; i < bArray.length; i++) {
	     sTemp = Integer.toHexString(0xFF & bArray[i]);
	     if (sTemp.length() < 2)
	      sb.append(0);
	     sb.append(sTemp.toUpperCase());
	    }
	    return sb.toString();
    }
    /** *//**
    * 把字节数组转换为对象
    * @param bytes
    * @return
    * @throws IOException
    * @throws ClassNotFoundException
    */
    public static final Object bytesToObject(byte[] bytes) throws IOException, ClassNotFoundException {
	    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
	    ObjectInputStream oi = new ObjectInputStream(in);
	    Object o = oi.readObject();
	    oi.close();
	    return o;
    }
    
    /** *//**
    * 把可序列化对象转换成字节数组
    * @param s
    * @return
    * @throws IOException
    */
    public static final byte[] objectToBytes(Serializable s) throws IOException {
	    ByteArrayOutputStream out = new ByteArrayOutputStream();
	    ObjectOutputStream ot = new ObjectOutputStream(out);
	    ot.writeObject(s);
	    ot.flush();
	    ot.close();
	    return out.toByteArray();
    }
    public static final String objectToHexString(Serializable s) throws IOException{
    	return bytesToHexString(objectToBytes(s));
    }
    public static final Object hexStringToObject(String hex) throws IOException, ClassNotFoundException{
    	return bytesToObject(hexStringToByte(hex));
    }
    
    /** *//**
    * @函数功能: BCD码转为10进制串(阿拉伯数据)
    * @输入参数: BCD码
    * @输出结果: 10进制串
    */
    public static String bcd2Str(byte[] bytes){
	    StringBuffer temp=new StringBuffer(bytes.length*2);
	    for(int i=0;i<bytes.length;i++){
	     temp.append((byte)((bytes[i]& 0xf0)>>>4));
	     temp.append((byte)(bytes[i]& 0x0f));
	    }
	    return temp.toString().substring(0,1).equalsIgnoreCase("0")?temp.toString().substring(1):temp.toString();
    }
    /** *//**
    * @函数功能: 10进制串转为BCD码
    * @输入参数: 10进制串
    * @输出结果: BCD码
    */
    public static byte[] str2Bcd(String asc) {
	    int len = asc.length();
	    int mod = len % 2;
	    if (mod != 0) {
	     asc = "0" + asc;
	     len = asc.length();
	    }
	    byte abt[] = new byte[len];
	    if (len >= 2) {
	     len = len / 2;
	    }
	    byte bbt[] = new byte[len];
	    abt = asc.getBytes();
	    int j, k;
	    for (int p = 0; p < asc.length()/2; p++) {
	     if ( (abt[2 * p] >= '0') && (abt[2 * p] <= '9')) {
	      j = abt[2 * p] - '0';
	     } else if ( (abt[2 * p] >= 'a') && (abt[2 * p] <= 'z')) {
	      j = abt[2 * p] - 'a' + 0x0a;
	     } else {
	      j = abt[2 * p] - 'A' + 0x0a;
	     }
	     if ( (abt[2 * p + 1] >= '0') && (abt[2 * p + 1] <= '9')) {
	      k = abt[2 * p + 1] - '0';
	     } else if ( (abt[2 * p + 1] >= 'a') && (abt[2 * p + 1] <= 'z')) {
	      k = abt[2 * p + 1] - 'a' + 0x0a;
	     }else {
	      k = abt[2 * p + 1] - 'A' + 0x0a;
	     }
	     int a = (j << 4) + k;
	     byte b = (byte) a;
	     bbt[p] = b;
	    }
	    return bbt;
    }
    
    /** *//**
    * @函数功能: BCD码转ASC码
    * @输入参数: BCD串
    * @输出结果: ASC码
    */
    //public static String BCD2ASC(byte[] bytes) {
	//    StringBuffer temp = new StringBuffer(bytes.length * 2);
	//    for (int i = 0; i < bytes.length; i++) {
	//     int h = ((bytes[i] & 0xf0) >>> 4);
	//     int l = (bytes[i] & 0x0f);   
	//     temp.append(BToA[h]).append( BToA[l]);
	//    }
	//    return temp.toString() ;
    //}
/** *//**
    * MD5加密字符串，返回加密后的16进制字符串
    * @param origin
    * @return
    */
    public static String MD5EncodeToHex(String origin) { 
       return bytesToHexString(MD5Encode(origin));
     }
    /** *//**
    * MD5加密字符串，返回加密后的字节数组
    * @param origin
    * @return
    */
    public static byte[] MD5Encode(String origin){
    	return MD5Encode(origin.getBytes());
    }
    /** *//**
    * MD5加密字节数组，返回加密后的字节数组
    * @param bytes
    * @return
    */
    public static byte[] MD5Encode(byte[] bytes){
	    MessageDigest md=null;
	    try {
	     md = MessageDigest.getInstance("MD5");
	     return md.digest(bytes);
	    } catch (NoSuchAlgorithmException e) {
	     e.printStackTrace();
	     return new byte[0];
	    }
    }
    
    
    public static void main(String[] args)
	{	
    	/*
    	int MessageType = Integer.parseInt("8006",16);
    	//byte[] bytes = CommonFunc.hexStringToByte("2020323031332D3130");
    	//String str = new String(bytes);
    	byte[] bytes = new String("\r").getBytes();
    	String str = CommonFunc.bytesToHexString(bytes);
    	int TotalLength = Integer.parseInt("011F",16);
    	System.out.print(str);*/
	}

}

 