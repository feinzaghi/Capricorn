package com.turk.clusters.master;

import com.turk.clusters.model.Register;

/**
 * 
 * �ڵ�ر�֪ͨ
 * <������ϸ����>
 * 
 * @author  Turk
 * @version  [�汾��, 2017��1��24��]
 * @see  [�����/����]
 * @since  [��Ʒ/ģ��汾]
 */
public class NodeShutdown extends AbstractMasterExecute{

	@Override
	public String Execute(String msgBody) {
		// TODO Auto-generated method stub
		Register reg3 = new Register();
		TaskManage.getInstance().UpdateSlaveStatus(reg3.getByJson(msgBody));
		String strReturn = "Done";
		return strReturn;
	}

}
