package com.turk.clusters.common;

/**
 * 
 * �ڵ�ִ�з����ӿ�
 * 
 * @author  Turk
 * @version  [�汾��, 2017��1��23��]
 * @see  [�����/����]
 * @since  [��Ʒ/ģ��汾]
 */
public interface IExecute {

	/**
	 * 
	 * ִ�з���
	 * <������ϸ����>
	 * @param msgBody
	 * @return ����ִ����ɺ��������� Ĭ����� Done���ش���client�ˡ�
	 * @see [�ࡢ��#��������#��Ա]
	 */
	String Execute(String msgBody);
}
