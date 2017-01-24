package com.turk.clusters.master;

import com.turk.clusters.model.Register;

/**
 * 
 * Slave节点注册消息
 * <功能详细描述>
 * 
 * @author  姓名 工号
 * @version  [版本号, 2017年1月24日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
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
