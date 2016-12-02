package com.turk.parser.cdr.hw;


public class SDUCHRFILEHEAD {

	/**
	 * �ļ��Ƿ���õı�־
	 */
	public int dwFlag;  //�ļ��Ƿ���õı�־
	/**
	 * ������ԪMU������
	 */
	public int dwMuNum; //������ԪMU������
	/**
	 * �ļ���ʱ����Ϣ
	 */
	public MuFileTimeInfo muTime; //�ļ���ʱ����Ϣ
	
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


//������Ԫ��ʱ����Ϣ
class MuFileTimeInfo
{
	/**
	 * ͳ������(��)
	 */
	public long period; //ͳ������(��)
	/**
	 * ��ʼʱ��
	 */
	public MuDstTimeType StartTime; //��ʼʱ��
	/**
	 * ����ʱ��
	 */
	public MuDstTimeType EndTime; //����ʱ��
	
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

//��ʼʱ�䣬����ʱ��
class MuDstTimeType
{
	/**
	 * ����ʱ��
	 */
	MuFileDateTimeType dt; //����ʱ��
	/**
	 * �Ƿ�ִ������ʱ
	 */
	byte isDst; //�Ƿ�ִ������ʱ
	/**
	 * ʱ��
	 */
	short timezone; //ʱ�� short
	/**
	 * ����ʱƫ��
	 */
	short DstOffset; //����ʱƫ�� short
	
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
	short year; //��
	byte month; //��
	byte day; //��
	byte hour; //Сʱ
	byte minute; //����
	byte second; //����
	
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