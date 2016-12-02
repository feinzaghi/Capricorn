package com.turk.parser;

/**
 * 不解析，空运行
 * 若不解析，则不会提交分发入库
 * @author Turk
 *
 */
public class NonParser extends Parser{

	@Override
	public boolean parseData() throws Exception {
		// TODO Auto-generated method stub
		
		return true;
	}

	@Override
	public void Stop() {
		// TODO Auto-generated method stub
		
	}

}
