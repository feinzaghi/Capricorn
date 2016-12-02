package com.turk.parser.cdr.hw;

public class MuResultInfo {
	public short MUID;
	public int RecordNum;
	public int Mulength;
	/**
	 * MI的数量
	 */
	short MINum;
	//public MITypeVecInfo MIList;
	//byte MORst[]  实际的行记录了
	
	public void setBuf(byte[] buf) {
		// TODO Auto-generated method stub
		byte[] temp = new byte[2];
		System.arraycopy(buf, 0, temp, 0, temp.length);
		MUID = CommonFunc.getShort(temp,0);
			
		temp = new byte[4];
		System.arraycopy(buf, 2, temp, 0, temp.length);
		RecordNum = CommonFunc.bytesToInt(temp);
		
		temp = new byte[4];
		System.arraycopy(buf, 6, temp, 0, temp.length);
		Mulength = CommonFunc.bytesToInt(temp);
		 
		temp = new byte[2];
		System.arraycopy(buf, 10, temp, 0, temp.length);
		MINum = CommonFunc.getShort(temp,0);
		//MIList = new MITypeVecInfo();
		//MIList = MIList.setBuf(buf);
	}
}


class MITypeVecInfo
{
	MIInfo[] MIList;
	
	public MITypeVecInfo setBuf(byte[] buf,short MINum) {
		// TODO Auto-generated method stub
		//byte[] temp = new byte[2];
		//System.arraycopy(buf, 10, temp, 0, temp.length);
		//MINum = CommonFunc.getShort(temp,0);
			
			 
		MIInfo info = new MIInfo();
		MIList = info.setBuf(buf, MINum, 0);
		
		return this;
	}
}

class MIInfo
{//7
	short MIID;
	byte MIType;
	int MILength;
	
	//int length;
	
	public MIInfo[] setBuf(byte[] buf,short MINum,int index) {
		// TODO Auto-generated method stub
		int startpos = index;
		MIInfo[] infolist = new MIInfo[MINum];
		for(int i = 0;i < MINum ; i++)
		{
			MIInfo info = new MIInfo();
			byte[] temp = new byte[2];
			System.arraycopy(buf, startpos, temp, 0, temp.length);
			info.MIID = CommonFunc.getShort(temp,0);
				
			temp = new byte[1];
			startpos = startpos + 2;
			System.arraycopy(buf, startpos, temp, 0, temp.length);
			info.MIType = temp[0];
			
			temp = new byte[4];
			startpos = startpos + 1;
			System.arraycopy(buf, startpos, temp, 0, temp.length);
			info.MILength = CommonFunc.bytesToInt(temp);

			startpos = startpos + 4;
			//length = startpos + 4;
			
			infolist[i] = info;
		}
		return infolist;
	}

}