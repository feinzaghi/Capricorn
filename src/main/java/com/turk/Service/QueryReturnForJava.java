package com.turk.Service;

import java.io.Serializable;

import org.apache.axis.types.Schema;

public class QueryReturnForJava implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 3434354541L;

	/// <summary>
    /// ��ѯ��Ϣ
    /// </summary>
    public String Message;

    /// <summary>
    /// ��ѯ�����
    /// </summary>
    public Schema ResultSet;

    /// <summary>
    /// ���ؽ����¼��
    /// </summary>
    public int ReturnCount;
    /// <summary>
    /// ��ѯ״̬
    /// </summary>
    public String Status;
}
