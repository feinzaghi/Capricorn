package com.turk.clusters.common;

/**
 * 
 * 节点执行方法接口
 * 
 * @author  Turk
 * @version  [版本号, 2017年1月23日]
 * @see  [相关类/方法]
 * @since  [产品/模块版本]
 */
public interface IExecute {

	/**
	 * 
	 * 执行方法
	 * <功能详细描述>
	 * @param msgBody
	 * @return 返回执行完成后的输出内容 默认输出 Done，回传给client端。
	 * @see [类、类#方法、类#成员]
	 */
	String Execute(String msgBody);
}
