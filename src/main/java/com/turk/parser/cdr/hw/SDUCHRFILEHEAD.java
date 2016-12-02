package com.turk.parser.cdr.hw;


public class SDUCHRFILEHEAD {

	/**
	 * 文件是否完好的标志
	 */
	public int dwFlag;  //文件是否完好的标志
	/**
	 * 测量单元MU的数量
	 */
	public int dwMuNum; //测量单元MU的数量
	/**
	 * 文件的时间信息
	 */
	public MuFileTimeInfo muTime; //文件的时间信息
	
	public void setBuf(byte[] buf) {
		// TODO Auto-generated method stub
		byte[] temp = new byte[4];
		System.arraycopy(buf, 0, temp, 0, temp.length);
		dwFlag = CommonFunc.bytesToInt(temp);
			
		temp = new byte[4];
		System.arraycopy(buf, 4, temp, 0, temp.length);
		dwMuNum = CommonFunc.bytesToInt(temp);
		
		 
		muTime = new MuFileTimeInfo();
		muTime = muTime.setBuf(buf);
	}
}


//测量单元的时间信息
class MuFileTimeInfo
{
	/**
	 * 统计周期(秒)
	 */
	public long period; //统计周期(秒)
	/**
	 * 开始时间
	 */
	public MuDstTimeType StartTime; //开始时间
	/**
	 * 结束时间
	 */
	public MuDstTimeType EndTime; //结束时间
	
	public MuFileTimeInfo setBuf(byte[] buf) {
		// TODO Auto-generated method stub
		byte[] temp = new byte[4];
		System.arraycopy(buf, 8, temp, 0, temp.length);
		period = CommonFunc.bytesToInt(temp); //8
		
		StartTime = new MuDstTimeType();
		StartTime = StartTime.setBuf(buf,12); //15+1
		EndTime = new MuDstTimeType();
		EndTime = EndTime.setBuf(buf,StartTime.length);
		return this;
	}
}

//开始时间，结束时间
class MuDstTimeType
{
	/**
	 * 日期时间
	 */
	MuFileDateTimeType dt; //日期时间
	/**
	 * 是否执行夏令时
	 */
	byte isDst; //是否执行夏令时
	/**
	 * 时区
	 */
	short timezone; //时区 short
	/**
	 * 夏令时偏移
	 */
	short DstOffset; //夏令时偏移 short
	
	int length = 0;
	
	public MuDstTimeType setBuf(byte[] buf,int index) {
		// TODO Auto-generated method stub
		dt = new MuFileDateTimeType();
		dt = dt.setBuf(buf,index); //15
		
		int startpos = dt.length;
		byte[] temp = new byte[1];
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		isDst = temp[0];
		
		temp = new byte[2];
		startpos = startpos + 1;
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		timezone =   CommonFunc.getShort(temp,0);
		
		temp = new byte[2];
		startpos = startpos + 2;
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		DstOffset =   CommonFunc.getShort(temp,0);
		
		length =  startpos + 2;
		return this;
	}
}

class MuFileDateTimeType
{
	short year; //年
	byte month; //月
	byte day; //日
	byte hour; //小时
	byte minute; //分钟
	byte second; //秒钟
	
	int length = 0;
	
	public MuFileDateTimeType setBuf(byte[] buf,int index) {
		// TODO Auto-generated method stub
		
		int startpos = index;
		byte[] temp = new byte[2];
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		year = CommonFunc.getShort(temp,0);
		
		temp = new byte[1];
		startpos = startpos + 2;
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		month = temp[0];
		
		temp = new byte[1];
		startpos = startpos + 1;
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		day = temp[0];
		
		temp = new byte[1];
		startpos = startpos + 1;
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		hour = temp[0];
		
		temp = new byte[1];
		startpos = startpos + 1;
		System.arraycopy(buf, startpos, temp, 0, temp.length);
		minute = temp[0];
		
		temp = new byte[1];
		startpos = startpos + 1;
		System.arraycopy(buf, startpos, temp, 0, temp.length); 
		second = temp[0];
		
		length = startpos + 1;
		return this;
	}
};