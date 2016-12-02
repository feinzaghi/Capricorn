package com.turk.console.commands;

import java.util.HashMap;

import org.apache.log4j.Logger;

import com.turk.console.common.console.io.CommandIO;
import com.turk.parser.taurus.MapImsiMsisdn;
import com.turk.specialapp.taurus.utele.MessageQueue;
import com.turk.specialapp.taurus.utele.RequestMsgInternal;

import com.turk.util.LogMgr;
import com.turk.util.Util;

/**
 * Taurus ��Ŀ�鿴�û��������
 * @author Administrator
 *
 */
public class TaurusmonitorCommand extends BasicCommand{

	//���з�
	private String lineSeparator = (String) java.security.AccessController.doPrivileged( 
            new sun.security.action.GetPropertyAction("line.separator"));

	private Logger tauruslog = LogMgr.getInstance().getAppLogger("taurus");
	
	
	
	@Override
	public boolean doCommand(String[] args,
			CommandIO io) throws Exception {
		// TODO Auto-generated method stub
		
		if ((args == null) || (args.length < 1))
		{
			io.println("syntax errors. input help /? for Command Help");
			return true;
		}
		
		if(args[0].trim().toLowerCase().equals("-m"))
		{
			HashMap<Integer,RequestMsgInternal> list 
				= MessageQueue.getInstance().GetAllQueue();
			StringBuffer sb = new StringBuffer();
			for(Integer i:list.keySet())
			{
				RequestMsgInternal msg = list.get(i);
				sb.append(String.format("QID:%d,key:%s,value:%s,client:%s %s", 
						i,msg.getKey(),msg.getValue(),
						msg.getSocket().getInetAddress() + ":" + msg.getSocket().getPort()
						, lineSeparator));
			}
			sb.append("the " + list.size() + " request.");
			io.println(sb.toString());
		}
		else if(args[0].trim().toLowerCase().equals("-c"))
		{
			StringBuffer sb = new StringBuffer();
			MapImsiMsisdn.getInstance().Clear();
			sb.append("clear imsi&msisdn map success.");
			io.println(sb.toString());
		}
		else if(args[0].trim().toLowerCase().equals("-k"))
		{
			if(args.length == 2)
			{
				String str = Util.findByRegex(args[1].trim().toLowerCase(), "[0-9]*", 0);
				int QID = -1;
				if(str!=null)
				{
					QID = Integer.parseInt(args[1].trim().toLowerCase());
				}
				
				if(QID == -1)
				{
					io.println("���б�����벻��ȷ");
					return true;
				}
				
				String strLine = io.readLine("ȷ����ֹ���б��Ϊ   "+ QID +" �ļ������?[y|n]");

		    	if ((strLine.equalsIgnoreCase("n")) || 
		    			(strLine.equalsIgnoreCase("no"))) {
		    		return true;
		    	}
		    	if ((!strLine.equalsIgnoreCase("n")) && 
		    			(!strLine.equalsIgnoreCase("no")) && 
		    			(!strLine.equalsIgnoreCase("y")) && 
		    			(!strLine.equalsIgnoreCase("yes")))
		    	{
		    		io.println("�Ƿ�����");
		    			return true;
		    	}
		    		
		    	if(!MessageQueue.getInstance().GetAllQueue().containsKey(QID))
		    	{
		    		io.println("�����ڱ��Ϊ  "+ QID +" �Ķ���");
	    			return true;
		    	}
				
				MessageQueue.getInstance().Remove(QID);
				
				tauruslog.debug("��ֹ�������"+ QID);
			}
		}
		return true;
	}

}
