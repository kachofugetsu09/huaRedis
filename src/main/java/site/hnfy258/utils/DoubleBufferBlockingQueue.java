package site.hnfy258.utils;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class DoubleBufferBlockingQueue implements BlockingQueue<ByteBuffer> {
    private final int bufferSize;
    private ByteBuffer currentBuffer;
    private ByteBuffer flushingBuffer;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notFull = lock.newCondition();
    private final Condition notEmpty = lock.newCondition();

    private volatile boolean closed = false;

    private volatile long writePosition = 0;
    private volatile long flushPosition = 0;



    public DoubleBufferBlockingQueue(int bufferSize) {
        this.bufferSize = bufferSize;
        this.currentBuffer = ByteBuffer.allocateDirect(bufferSize);
        this.flushingBuffer = ByteBuffer.allocateDirect(bufferSize);
    }

    @Override
    public void put(ByteBuffer src) throws InterruptedException {
        if (closed) {
            throw new IllegalStateException("Queue is closed");
        }

        lock.lock();
        try {
            int requiredSpace = src.remaining();

            // 如果当前缓冲区空间不足，等待刷新
            while (currentBuffer.remaining() < requiredSpace) {
                // 如果缓冲区太小无法放入数据，抛出异常
                if (requiredSpace > bufferSize) {
                    throw new IllegalArgumentException(
                            "Buffer too large: " + requiredSpace + " bytes, max is " + bufferSize);
                }

                // 如果当前缓冲区已使用空间超过一半，主动触发交换
                if (currentBuffer.position() > bufferSize / 2) {
                    swapArea();
                    notFull.signal();
                } else {
                    // 否则等待空间变得可用
                    notFull.await();
                    if (closed) {
                        throw new IllegalStateException("Queue is closed");
                    }
                }
            }

            // 记录写入前的位置，用于跟踪
            int beforePos = currentBuffer.position();

            // 执行数据写入
            currentBuffer.put(src);

            // 更新写入位置
            writePosition += (currentBuffer.position() - beforePos);

            // 通知有新数据可用
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }


    @Override
    public ByteBuffer take() throws InterruptedException {
        lock.lock();
        try {
            while (currentBuffer.position() == 0 && !closed) {
                notEmpty.await();
            }

            if (closed && currentBuffer.position() == 0) {
                return null;
            }

            swapArea();

            notFull.signal();
            return flushingBuffer;
        } finally {
            lock.unlock();
        }
    }

    private void swapArea() {
        // 交换缓冲区
        ByteBuffer temp = flushingBuffer;
        flushingBuffer = currentBuffer;
        currentBuffer = temp;

        // 准备返回的缓冲区
        flushingBuffer.flip();
        currentBuffer.clear();

        flushPosition = writePosition;
    }

    public void close() {
        lock.lock();
        try {
            closed = true;
            notEmpty.signalAll();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(ByteBuffer byteBuffer) {
        if (closed) {
            return false;
        }

        lock.lock();
        try {
            if (currentBuffer.remaining() < byteBuffer.remaining()) {
                return false;
            }
            currentBuffer.put(byteBuffer);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(ByteBuffer byteBuffer, long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return false;
        }

        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (currentBuffer.remaining() < byteBuffer.remaining()) {
                if (nanos <= 0) {
                    return false;
                }
                nanos = notFull.awaitNanos(nanos);
                if (closed) {
                    return false;
                }
            }
            currentBuffer.put(byteBuffer);
            notEmpty.signal();
            return true;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer poll() {
        lock.lock();
        try {
            if (currentBuffer.position() == 0) {
                return null;
            }

            // 交换缓冲区
            swapArea();

            notFull.signal();
            return flushingBuffer;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ByteBuffer poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (currentBuffer.position() == 0 && !closed) {
                if (nanos <= 0) {
                    return null;
                }
                nanos = notEmpty.awaitNanos(nanos);
            }

            if (closed && currentBuffer.position() == 0) {
                return null;
            }

            // 交换缓冲区
            swapArea();

            notFull.signal();
            return flushingBuffer;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        lock.lock();
        try {
            return currentBuffer.remaining();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super ByteBuffer> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super ByteBuffer> c, int maxElements) {
        if (c == null) throw new NullPointerException();
        if (c == this) throw new IllegalArgumentException();
        if (maxElements <= 0) return 0;

        lock.lock();
        try {
            int n = Math.min(maxElements, 1); // 我们每次最多只能取一个缓冲区

            if (currentBuffer.position() == 0) {
                return 0;
            }

            // 交换缓冲区
            swapArea();

            c.add(flushingBuffer);
            notFull.signal();
            return n;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int size() {
        lock.lock();
        try {
            return currentBuffer.position();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return currentBuffer.position() == 0;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        return false; // 不支持此操作
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public <T> T[] toArray(T[] a) {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public boolean remove(Object o) {
        return false; // 不支持此操作
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return false; // 不支持此操作
    }

    @Override
    public boolean addAll(Collection<? extends ByteBuffer> c) {
        throw new UnsupportedOperationException(); // 不支持此操作
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return false; // 不支持此操作
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return false; // 不支持此操作
    }

    @Override
    public void clear() {
        lock.lock();
        try {
            currentBuffer.clear();
            flushingBuffer.clear();
            notFull.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean add(ByteBuffer byteBuffer) {
        if (offer(byteBuffer)) {
            return true;
        }
        throw new IllegalStateException("Queue full");
    }

    @Override
    public ByteBuffer remove() {
        ByteBuffer buffer = poll();
        if (buffer == null) {
            throw new IllegalStateException("Queue empty");
        }
        return buffer;
    }

    @Override
    public ByteBuffer element() {
        ByteBuffer buffer = peek();
        if (buffer == null) {
            throw new IllegalStateException("Queue empty");
        }
        return buffer;
    }

    @Override
    public ByteBuffer peek() {
        lock.lock();
        try {
            if (currentBuffer.position() == 0) {
                return null;
            }
            // 返回当前缓冲区的只读视图
            ByteBuffer view = currentBuffer.duplicate();
            view.flip();
            return view;
        } finally {
            lock.unlock();
        }
    }

    public long getWritePosition() {
        return writePosition;
    }

    /**
     * 获取最后刷盘位置
     */
    public long getFlushPosition() {
        return flushPosition;
    }

    /**
     * 获取未刷盘数据大小
     */
    public long getUnflushedSize() {
        return writePosition - flushPosition;
    }
}