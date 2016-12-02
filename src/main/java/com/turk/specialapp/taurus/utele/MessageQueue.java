package com.turk.specialapp.taurus.utele;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import com.turk.util.LogMgr;

/**
 * 客户端请求的消息队列
 * @author Administrator
 *
 */
public class MessageQueue {
	
	private int _msgIndex = 0;
	
	private Logger log = LogMgr.getInstance().getSystemLogger();
	
	private static MessageQueue _instance = null;
	
	private HashMap<Integer,RequestMsgInternal> _list = new HashMap<Integer, RequestMsgInternal>();
	
	/*imsi map*/
	private HashMap<String,List<Integer>> mImsiMap = new HashMap<String, List<Integer>>();
	
	private HashMap<String,List<Integer>> mMsisdnMap = new HashMap<String, List<Integer>>();
	
	
	public static synchronized MessageQueue getInstance()
	{
		if(_instance == null)
			_instance = new MessageQueue();
		
		return _instance;
	}
	
	/**
	 * 将客户端请求加入消息队列
	 * @param msgObj
	 */
	public synchronized int Add(RequestMsgInternal msgObj)
	{
		//设置消息队列编号
		//msgObj.setQueueNum(_msgIndex);
		_msgIndex++;
		
		_list.put(_msgIndex, msgObj);
		log.debug("Add Monitor info:" + msgObj.getValue());
		String sKey = msgObj.getKey();
		String sValue = msgObj.getValue();
		
		String[] keys = sKey.split("$");
		String[] values = sValue.split("$");
		
		for(int i=0;i<keys.length;i++)
		{
			String[] value = values[i].split(",");
			for(int j=0;j<value.length;j++)
			{
				if(keys[i].toUpperCase().equals("IMSI"))
				{
					if(mImsiMap.containsKey(value[j]))
					{
						List<Integer> list = mImsiMap.get(value[j]);
						list.add(_msgIndex);
					}
					else
					{
						List<Integer> list = new ArrayList<Integer>();
						list.add(_msgIndex);
						mImsiMap.put(value[j], list);
					}
				}
				
				if(keys[i].toUpperCase().equals("MSISDN"))
				{
					if(mMsisdnMap.containsKey(value[j]))
					{
						List<Integer> list = mMsisdnMap.get(value[j]);
						list.add(_msgIndex);
					}
					else
					{
						List<Integer> list = new ArrayList<Integer>();
						list.add(_msgIndex);
						mMsisdnMap.put(value[j], list);
					}
				}
			}
		}
		
		return _msgIndex;
	}
	
	/**
	 * 移除消息
	 * @param queueNum
	 */
	public void Remove(int queueNum)
	{
		if(_list.containsKey(queueNum))
		{
			RequestMsgInternal msgObj = _list.get(queueNum);
			log.debug("Remove Monitor info:" + msgObj.getValue() + " client info:" + msgObj.getServer());
			_list.remove(queueNum);
			
		}
		
		
		for(String key :  mImsiMap.keySet())
		{
			List<Integer> list = mImsiMap.get(key);
			if(list == null)
				continue;
			for(int i = 0;i<list.size();i++)
			{
				if(queueNum == list.get(i))
				{
					list.remove(i);
					break;
				}
			}
		}
		
		for(String key : mMsisdnMap.keySet())
		{
			List<Integer> list = mMsisdnMap.get(key);
			if(list==null)
				continue;
			for(int i = 0;i<list.size();i++)
			{
				if(queueNum == list.get(i))
				{
					list.remove(i);
					break;
				}
			}
		}
		
		
	}
	
	/**
	 * 清理队列
	 */
	public void Clear()
	{
		_list.clear();
		mMsisdnMap.clear();
		mImsiMap.clear();
	}
	
	/**
	 * 全部请求队列
	 * @return
	 */
	public HashMap<Integer,RequestMsgInternal> GetAllQueue()
	{
		return _list;
	}
	
	/**
	 * IMSI 监听队列
	 * @return
	 */
	public HashMap<String,List<Integer>> GetImsiQueue()
	{
		return this.mImsiMap;
	}
	
	/**
	 * MSISDN 监听队列
	 * @return
	 */
	public HashMap<String,List<Integer>> GetMsisdnQueue()
	{
		return this.mMsisdnMap;
	}
	
}
