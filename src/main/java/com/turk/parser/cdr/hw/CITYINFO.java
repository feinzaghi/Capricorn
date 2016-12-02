package com.turk.parser.cdr.hw;

public class CITYINFO {
	public int   nCityId;
	public int   uStartId;//城市区块编号开始位置
	public double dwMinLat; //保留城市最小纬度坐标
	public double dwMaxLat; //保留城市最大纬度坐标
	public double dwMinLon; //保留城市最小经度坐标
	public double dwMaxLon; //保留城市最大经度坐标
	public double dwLat_100;//保留一百米距离平均纬度长度
	public double dwLon_100;//保留一百米距离平均经度长度
	public double dwWidth;//城市总长度,按照经度来计算
	public double dwHeigh;//城市总高度,按照纬度来计算
	public int   nGridM,nGridN;
	public int   nSystemID;
	public int    nGridSquare; //城市栅格统计距离
}
