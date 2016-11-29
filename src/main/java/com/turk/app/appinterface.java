package com.turk.app;

public interface appinterface {
	
	/**
	 * 启动应用
	 */
	void StartApp();
	
	/**
	 * 获取单例实例
	 * @return
	 */
	appinterface getInstance();
	
	/**
	 * 关闭应用
	 */
	void Shutdown();
}
