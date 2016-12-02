package com.turk.specialapp.taurus.utele;

import java.net.Socket;

/**
 * web request internal
 * @author Administrator
 *
 */
public class RequestMsgInternal extends RequestMsg{

	private Socket _socket;
	
	public void setSocket(Socket socket)
	{
		this._socket = socket;
	}
	
	public Socket getSocket()
	{
		return this._socket;
	}
	
}
