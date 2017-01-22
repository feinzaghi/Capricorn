package com.turk.rpc;

/**
 * 客户端从服务端获取消息之后的回调
 * @author Administrator
 *
 */
public interface IMessageCallBack {
	void ReturnMessage(String msg);
}
