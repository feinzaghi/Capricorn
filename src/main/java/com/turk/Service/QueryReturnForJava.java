package com.turk.Service;

import java.io.Serializable;

import org.apache.axis.types.Schema;

public class QueryReturnForJava implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3434354541L;

	/// <summary>
    /// 查询信息
    /// </summary>
    public String Message;

    /// <summary>
    /// 查询结果集
    /// </summary>
    public Schema ResultSet;

    /// <summary>
    /// 返回结果记录数
    /// </summary>
    public int ReturnCount;
    /// <summary>
    /// 查询状态
    /// </summary>
    public String Status;
}
