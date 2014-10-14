package com.ucjoy.magic.util;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

/**
 * 创建人：sp.fu <br>
 * 功能描述：双缓冲工作队列，线程安全 <br>
 * 版本： <br>
 * ====================================== <br>
 *               修改记录 <br>
 * ====================================== <br>
 *  序号    姓名      日期      版本           简单描述 <br>
 *
 */

public class DoubleQueue<E> extends AbstractQueue<E> implements
		BlockingQueue<E>, java.io.Serializable {
	private static final long serialVersionUID = 1011398447523020L;
	public static final int DEFALUT_QUEUE_CAPACITY = 50000;
	private Logger logger = Logger.getLogger(DoubleQueue.class.getName());
	/** The read lock */
	private ReentrantLock readLock;
	//写锁
	private ReentrantLock writeLock;
	//是否满,信号量
	private Condition notFull;
	private Condition awake;
    //读写数组
	private transient E[] writeArray;
	private transient E[] readArray;
	//读写计数
	private volatile int writeCount;
	private volatile int readCount;
	//写数组下标指针
	private int writeArrayTP; 
	private int writeArrayHP;
	//读数组下标指针
	private int readArrayTP;
	private int readArrayHP;
	private int capacity;

	public DoubleQueue(int capacity) {
		//默认
		this.capacity = DEFALUT_QUEUE_CAPACITY;
		
		if (capacity > 0) {
			this.capacity = capacity;
		}
      
		readArray = (E[])new Object[capacity];
		writeArray =  (E[])new Object[capacity];

		readLock = new ReentrantLock();
		writeLock = new ReentrantLock();
		notFull = writeLock.newCondition();
		awake = writeLock.newCondition();
	}

	private void insert(E e) {
		writeArray[writeArrayTP] = e;
		++writeArrayTP;
		++writeCount;
	}

	private E extract() {
		E e = readArray[readArrayHP];
		readArray[readArrayHP] = null;
		++readArrayHP;
		--readCount;
		return e;
	}

	/**
	 * 交换读写队列指针
	 * @param timeout
	 * @param isInfinite
	 * @throws InterruptedException
	 */
	private long queueSwap(long timeout, boolean isInfinite)
			throws InterruptedException {
		writeLock.lock();
		try {
			if (writeCount <= 0) {
				logger.debug("Write Count:" + writeCount
						+ ", Write Queue is empty, do not switch!");
				try {
					logger.debug("Queue is empty, need wait....");
					if (isInfinite && timeout <= 0) {
						awake.await();
						return -1;
					} else {
						return awake.awaitNanos(timeout);
					}
				} catch (InterruptedException ie) {
					awake.signal();
					throw ie;
				}
			} else {
				E[] tmpArray = readArray;
				readArray = writeArray;
				writeArray = tmpArray;

				readCount = writeCount;
				readArrayHP = 0;
				readArrayTP = writeArrayTP;

				writeCount = 0;
				writeArrayHP = readArrayHP;
				writeArrayTP = 0;

				notFull.signal();
				logger.debug("Queue switch successfully!");
				return -1;
			}
		} finally {
			writeLock.unlock();
		}
	}
	@Override
	public boolean offer(E e, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (e == null) {
			throw new NullPointerException();
		}

		long nanoTime = unit.toNanos(timeout);
		writeLock.lockInterruptibly();
		try {
			for (;;) {
				if (writeCount < writeArray.length) {
					insert(e);
					if (writeCount == 1) {
						awake.signal();
					}
					return true;
				}

				// Time out
				if (nanoTime <= 0) {
					logger.debug("offer wait time out!");
					return false;
				}
				// keep waiting
				try {
					logger.debug("Queue is full, need wait....");
					nanoTime = notFull.awaitNanos(nanoTime);
				} catch (InterruptedException ie) {
					notFull.signal();
					throw ie;
				}
			}
		} finally {
			writeLock.unlock();
		}
	}
	//取
	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		long nanoTime = unit.toNanos(timeout);
		readLock.lockInterruptibly();

		try {
			for (;;) {
				if (readCount > 0) {
					return extract();
				}

				if (nanoTime <= 0) {
					logger.debug("poll time out!");
					return null;
				}
				nanoTime = queueSwap(nanoTime, false);
			}
		} finally {
			readLock.unlock();
		}
	}
	//等待500毫秒
	@Override
	public E poll() {
		E ret = null;
		try {
			ret = poll(500L, TimeUnit.MILLISECONDS);
		} catch (Exception e) {
			ret = null;
		}
		return ret;
	}
	//查看
	@Override
	public E peek() {
		E e = null;
		readLock.lock();
        
		try {
			if (readCount > 0) {
				e = readArray[readArrayHP];
			}
		} finally {
			readLock.unlock();
		}
		
		return e;
	}

	//默认500毫秒
	@Override
	public boolean offer(E e) {
		boolean ret = false;
		try {
			ret = offer(e, 500L, TimeUnit.MILLISECONDS);
		} catch (Exception e2) {
			ret = false;
		}
		return ret;
	}

	@Override
	public void put(E e) throws InterruptedException {
		offer(e, 500L, TimeUnit.MILLISECONDS); // never need to block
	}

	@Override
	public E take() throws InterruptedException {
		return poll(500L, TimeUnit.MILLISECONDS);
	}

	@Override
	public int remainingCapacity() {
		return  this.capacity;
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		return 0;
	}

	@Override
	public Iterator<E> iterator() {
		return null;
	}

	//当前读队列中还有多少个
	@Override
	public int size() {
		int size = 0;
		readLock.lock();
        
		try {
			if (readCount > 0) {
				size = readCount;
			}
		} finally {
			readLock.unlock();
		}
		
		return size;
	}
	//清理
	public void clear() {
		writeLock.lock();
		try {
			readCount = 0;
			readArrayHP = 0;
			writeCount = 0;
			writeArrayTP = 0;
			logger.debug("Queue clear successfully!");
		} finally {
			writeLock.unlock();
		}
	}
	
	public static void main(String[] args) {
		try {
			DoubleQueue<String> q = new DoubleQueue<String>(1000);
			q.put("shiping");
			q.put("fu");
			q.poll();
			System.out.println(q.size());
		} catch (InterruptedException e) {
			// Auto-generated catch block
			e.printStackTrace();
		}
	}
}