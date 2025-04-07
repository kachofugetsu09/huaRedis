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

    static final int MAXMIUM_CAPACITY = 1<<29;

    static final int MAXIMUM_SUBAREA = 1<<12;

    Object data[][];

    volatile int readIndex = -1;
    volatile int writeIndex = -1;

    private final AtomicInteger count = new AtomicInteger();

    //lock held by take poll

    private final ReentrantLock takeLock = new ReentrantLock();
    private final Condition notEmpty = takeLock.newCondition();
    private final ReentrantLock putLock = new ReentrantLock();
    private final Condition notFull = putLock.newCondition();



    private void signalNotEmpty(){
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


    public RingBlockingQueue(int subareaSize,int capacity) {
        this(subareaSize,capacity,1);
    }

    /**
     * 初始化队列
     * @param subareaSize 分片数
     * @param capacity 容量
     * @param concurrency 并发数
     */public RingBlockingQueue(int subareaSize, int capacity, int concurrency){
        if(subareaSize>MAXIMUM_SUBAREA||subareaSize<0||capacity<0||capacity>=MAXMIUM_CAPACITY){
            throw new IllegalArgumentException("Illegal init");
        }
        maxSize = capacity;
//        subareaSize = subareaSizeOf(subareaSize);
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
        return false;
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
        return null;
    }

    @Override
    public E peek() {
        return null;
    }
}
