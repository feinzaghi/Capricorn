package com.turk.clusters.master;

import com.turk.clusters.model.Register;

/**
 * 
 * Slave�ڵ�ע����Ϣ
 * <������ϸ����>
 * 
 * @author  ���� ����
 * @version  [�汾��, 2017��1��24��]
 * @see  [�����/����]
 * @since  [��Ʒ/ģ��汾]
 */
public class NodeRegister extends AbstractMasterExecute {

	@Override
	public String Execute(String msgBody) {
		// TODO Auto-generated method stub
		Register reg1 = new Register();
		TaskManage.getInstance().NodeRegister(reg1.getByJson(msgBody));
		String strReturn = "Done";
		return strReturn;
	}

}
