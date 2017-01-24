package com.turk.clusters.master;

import com.turk.clusters.model.Register;

/**
 * 
 * 节点同步报活
 * <功能详细描述>
 * 
 * @author  姓名 工号
 * @version  [版本号, 2017年1月24日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
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
