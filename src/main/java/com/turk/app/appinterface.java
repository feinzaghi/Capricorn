package com.turk.app;

public interface appinterface {
	
	/**
	 * ����Ӧ��
	 */
	void StartApp();
	
	/**
	 * ��ȡ����ʵ��
	 * @return
	 */
	appinterface getInstance();
	
	/**
	 * �ر�Ӧ��
	 */
	void Shutdown();
}
