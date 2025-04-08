package site.hnfy258.aof;

import java.io.Serializable;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class RingBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>, Serializable {

    static final int MAXIMUM_CAPACITY = 1 << 29;

    static final int MAXIMUM_SUBAREA = 1 << 12;

    //二维数组,用于存储队列中的元素。
    Object data[][];

    //读取元素的位置。
    volatile int readIndex = -1;
    //写入元素的位置。
    volatile int writeIndex = -1;

    private final AtomicInteger count = new AtomicInteger();

    //lock held by take poll

    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();


    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            //使线程等待，直到收到信号notempty
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }


    int capacity;

    int rowOffice;
    int columnOffice;
    int rowSize;
    int bitHigh;
    int subareaSize;
    int maxSize;


    public RingBlockingQueue(int subareaSize, int capacity) {
        this(subareaSize, capacity, 1);
    }

    /**
     * 初始化队列
     *
     * @param subareaSize 分片数
     * @param capacity    容量
     * @param concurrency 并发数
     */
    public RingBlockingQueue(int subareaSize, int capacity, int concurrency) {
        if (subareaSize > MAXIMUM_SUBAREA || subareaSize < 0 || capacity < 0 || capacity >= MAXIMUM_CAPACITY) {
            throw new IllegalArgumentException("Illegal init");
        }
        //
        maxSize = capacity;
        subareaSize = subareaSizeOf(subareaSize);
        capacity = tableSizeOf(capacity);
        rowSize = tableSizeOf(capacity / subareaSize);
        capacity = rowSize * capacity;

        data = new Object[rowSize][subareaSize];
        bitHigh = getIntHigh(subareaSize);
        this.capacity = capacity;
        this.subareaSize = subareaSize;
        rowOffice = rowSize - 1;
        columnOffice = subareaSize - 1;
    }


    public RingBlockingQueue(Collection<? extends E> c) {
        this(8888, 88888);
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            int n = 0;
            for (E e : c) {
                if (e == null) {
                    throw new NullPointerException();
                }
                if (n == capacity) {
                    throw new IllegalStateException("Queue full");
                }
                put(e);
                n++;
            }
            count.set(n);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            putLock.unlock();
        }
    }

    static final int tableSizeOf(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    static final int getIntHigh(int cap) {
        int high = 0;
        while ((cap & 1) == 0) {
            high++;
            cap = cap >> 1;
        }
        return high;
    }

    static final int subareaSizeOf(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return (n < 0) ? 1 : (n >= MAXIMUM_SUBAREA) ? MAXIMUM_SUBAREA : n + 1;
    }
    public void refreshIndex() {
        if(readIndex>capacity){
            putLock.lock();
            try{
                synchronized (this){
                    if(readIndex>capacity){
                        readIndex  -= capacity;
                        writeIndex -= capacity;
                    }
                }
            }finally {
                putLock.unlock();
            }
        }
    }







    @Override
    public Iterator<E> iterator() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean offer(E e) {
        int localWriteIndex = 0;
        synchronized (this){
            if(writeIndex>readIndex+capacity){
                return false;
            }
            count.incrementAndGet();
            localWriteIndex = ++writeIndex;
        }
        //行信息储存在高位
        int row = (localWriteIndex >> bitHigh)&rowOffice;
        //列信息储存在低位
        int column = localWriteIndex & columnOffice;
        if(column==0&&row==0){
            refreshIndex();
        }
        data[row][column] = e;
        return true;
    }

    @Override
    public void put(E e) throws InterruptedException {

    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        return null;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        return null;
    }

    @Override
    public int remainingCapacity() {
        return 0;
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
    public E poll() {
        int localReadIndex = 0;
        synchronized (this){
            if(writeIndex>readIndex){
                localReadIndex = ++readIndex;
                count.decrementAndGet();
            }else{
                return null;
            }
        }
        int row = (localReadIndex >> bitHigh)&rowOffice;
        int column = localReadIndex & columnOffice;
        if(column==0&&row==0){
            refreshIndex();
        }
        return (E)data[row][column];
    }

    @Override
    public E peek() {
        int localReadIndex=0;
        synchronized (this){
            if(writeIndex>readIndex){
                localReadIndex=readIndex;
            }else{
                return null;
            }
        }
        int row=(localReadIndex>>bitHigh)&(rowOffice);
        int column =localReadIndex&(columnOffice);
        if(column==0&&row==0){
            refreshIndex();
        }
        return (E)data[row][column];
    }
}
