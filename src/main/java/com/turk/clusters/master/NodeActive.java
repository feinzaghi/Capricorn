package com.turk.clusters.master;

import com.turk.clusters.model.Register;

/**
 * 
 * �ڵ�ͬ������
 * <������ϸ����>
 * 
 * @author  ���� ����
 * @version  [�汾��, 2017��1��24��]
 * @see  [�����/����]
 * @since  [��Ʒ/ģ��汾]
 */
public class NodeActive extends AbstractMasterExecute{

	@Override
	public String Execute(String msgBody) {
		// TODO Auto-generated method stub
		Register reg2 = new Register();
		TaskManage.getInstance().UpdateSlaveStatus(reg2.getByJson(msgBody));
		String strReturn = "Done";
		return strReturn;
	}

}
