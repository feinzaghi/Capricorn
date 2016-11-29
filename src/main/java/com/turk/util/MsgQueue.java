package com.turk.util;

import java.util.Deque;
import java.util.LinkedList;

public class MsgQueue<T>
{
	private Deque<T> queue;
	private static final int MAX_COUNT = 5000;

	public MsgQueue()
	{
		this.queue = new LinkedList();
	}

	public synchronized T get()
    	throws InterruptedException
    {
		while (this.queue.peek() == null)
		{
			wait();
		}

		notifyAll();
		return this.queue.poll();
    }

	public synchronized void put(T msg)
    	throws InterruptedException
    {
		if (msg == null) {
			return;
		}
		do
		{
			wait();
		}
		while (this.queue.size() == 5000);
		boolean bReturn = this.queue.offer(msg);
		if (bReturn)
			notifyAll();
    }

	public synchronized void putFirst(T msg)
    	throws InterruptedException
    {
		if (msg == null) {
			return;
		}
		do
		{
			wait();
		}
		while (this.queue.size() == 5000);
		boolean bReturn = this.queue.offerFirst(msg);
		if (bReturn)
			notifyAll();
    }

	public synchronized void clear()
	{
		this.queue.clear();
	}
}