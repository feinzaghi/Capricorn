package com.turk.clusters.master;

import com.turk.clusters.model.Register;

/**
 * 
 * 节点关闭通知
 * <功能详细描述>
 * 
 * @author  Turk
 * @version  [版本号, 2017年1月24日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
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
