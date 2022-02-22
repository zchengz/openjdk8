/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * This file is available under and governed by the GNU General Public
 * License version 2 only, as published by the Free Software Foundation.
 * However, the following notice accompanied the original version of this
 * file:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package java.util.concurrent;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * An optionally-bounded {@linkplain BlockingQueue blocking queue} based on
 * linked nodes.
 * This queue orders elements FIFO (first-in-first-out).
 * The <em>head</em> of the queue is that element that has been on the
 * queue the longest time.
 * The <em>tail</em> of the queue is that element that has been on the
 * queue the shortest time. New elements
 * are inserted at the tail of the queue, and the queue retrieval
 * operations obtain elements at the head of the queue.
 * Linked queues typically have higher throughput than array-based queues but
 * less predictable performance in most concurrent applications.
 *
 * <p>The optional capacity bound constructor argument serves as a
 * way to prevent excessive queue expansion. The capacity, if unspecified,
 * is equal to {@link Integer#MAX_VALUE}.  Linked nodes are
 * dynamically created upon each insertion unless this would bring the
 * queue above capacity.
 *
 * <p>This class and its iterator implement all of the <em>optional</em>
 * methods of the {@link Collection} and {@link Iterator} interfaces.
 *
 * <p>This class is a member of the
 * <a href="{@docRoot}/java.base/java/util/package-summary.html#CollectionsFramework">
 * Java Collections Framework</a>.
 *
 * @since 1.5
 * @author Doug Lea
 * @param <E> the type of elements held in this queue
 */
public class LinkedBlockingQueue<E> extends AbstractQueue<E>
        implements BlockingQueue<E>, java.io.Serializable {
    private static final long serialVersionUID = -6903933977591709194L;

    /*
     * A variant of the "two lock queue" algorithm.  The putLock gates
     * entry to put (and offer), and has an associated condition for
     * waiting puts.  Similarly for the takeLock.  The "count" field
     * that they both rely on is maintained as an atomic to avoid
     * needing to get both locks in most cases. Also, to minimize need
     * for puts to get takeLock and vice-versa, cascading notifies are
     * used. When a put notices that it has enabled at least one take,
     * it signals taker. That taker in turn signals others if more
     * items have been entered since the signal. And symmetrically for
     * takes signalling puts. Operations such as remove(Object) and
     * iterators acquire both locks.
     *
     * Visibility between writers and readers is provided as follows:
     *
     * Whenever an element is enqueued, the putLock is acquired and
     * count updated.  A subsequent reader guarantees visibility to the
     * enqueued Node by either acquiring the putLock (via fullyLock)
     * or by acquiring the takeLock, and then reading n = count.get();
     * this gives visibility to the first n items.
     *
     * To implement weakly consistent iterators, it appears we need to
     * keep all Nodes GC-reachable from a predecessor dequeued Node.
     * That would cause two problems:
     * - allow a rogue Iterator to cause unbounded memory retention
     * - cause cross-generational linking of old Nodes to new Nodes if
     *   a Node was tenured while live, which generational GCs have a
     *   hard time dealing with, causing repeated major collections.
     * However, only non-deleted Nodes need to be reachable from
     * dequeued Nodes, and reachability does not necessarily have to
     * be of the kind understood by the GC.  We use the trick of
     * linking a Node that has just been dequeued to itself.  Such a
     * self-link implicitly means to advance to head.next.
     */

    /**
     * Linked list node class.
     */
    static class Node<E> {
    	// 存储的数据
        E item;

        /**
         * One of:
         * - the real successor Node
         * - this Node, meaning the successor is head.next
         * - null, meaning there is no successor (this is the last node)
         */
        // 下一个元素
        Node<E> next;

        Node(E x) { item = x; }
    }

    /** The capacity bound, or Integer.MAX_VALUE if none */
	/** 容量 */
    private final int capacity;

    /** Current number of elements */
	/** 当前元素个数 因为入队出队不是一把锁 所以使用 AtomicInteger 保证技术线程安全 */
    private final AtomicInteger count = new AtomicInteger();

    /**
     * Head of linked list.
     * Invariant: head.item == null
     * 头结点 item 一直为null
     */
    transient Node<E> head;

    /**
     * Tail of linked list.
     * Invariant: last.next == null
     * 尾结点 last的next 一直为null
     */
    private transient Node<E> last;

    /** Lock held by take, poll, etc */
	/** 入队时的锁 因为是链表,入队时只是在尾部追加,所以使用两把锁提高效率*/
    private final ReentrantLock takeLock = new ReentrantLock();

    /** Wait queue for waiting takes */
	/** 不为空的等待条件 */
    @SuppressWarnings("serial") // Classes implementing Condition may be serializable.
    private final Condition notEmpty = takeLock.newCondition();

    /** Lock held by put, offer, etc */
	/** 出队时的锁 */
    private final ReentrantLock putLock = new ReentrantLock();

    /** Wait queue for waiting puts */
	/** 队列不满的等待条件 */
    @SuppressWarnings("serial") // Classes implementing Condition may be serializable.
    private final Condition notFull = putLock.newCondition();

    /**
     * Signals a waiting take. Called only from put/offer (which do not
     * otherwise ordinarily lock takeLock.)
     * 唤醒出队的等待线程
     */
    private void signalNotEmpty() {
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Signals a waiting put. Called only from take/poll.
     * 唤醒入队的等待线程
     */
    private void signalNotFull() {
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            notFull.signal();
        } finally {
            putLock.unlock();
        }
    }

    /**
     * Links node at end of queue.
     * 将node追加到尾部
     *
     * @param node the node
     */
    private void enqueue(Node<E> node) {
        // assert putLock.isHeldByCurrentThread();
        // assert last.next == null;
        // 等价于 last.next = node ; last = last.next;
        last = last.next = node;
    }

    /**
     * Removes a node from head of queue.
     *
     * 弹出队首node
     * @return the node
     */
    private E dequeue() {
        // assert takeLock.isHeldByCurrentThread();
        // assert head.item == null;
        Node<E> h = head;
        Node<E> first = h.next;
        h.next = h; // help GC 自己指向自己
        // 将弹出元素的node的item置为null,充当head节点
        head = first;
        E x = first.item;
        first.item = null;
        return x;
    }

    /**
     * Locks to prevent both puts and takes.
     * 全局上锁 两把锁都上锁
     */
    void fullyLock() {
        putLock.lock();
        takeLock.lock();
    }

    /**
     * Unlocks to allow both puts and takes.
     * 全局释放锁 释放两把锁
     */
    void fullyUnlock() {
        takeLock.unlock();
        putLock.unlock();
    }

    /**
     * Creates a {@code LinkedBlockingQueue} with a capacity of
     * {@link Integer#MAX_VALUE}.
     * 默认容量为Integer.MAX_VALUE
     */
    public LinkedBlockingQueue() {
        this(Integer.MAX_VALUE);
    }

    /**
     * Creates a {@code LinkedBlockingQueue} with the given (fixed) capacity.
     *
     * @param capacity the capacity of this queue
     * @throws IllegalArgumentException if {@code capacity} is not greater
     *         than zero
     */
    public LinkedBlockingQueue(int capacity) {
    	// 参数合法性校验
        if (capacity <= 0) throw new IllegalArgumentException();
        this.capacity = capacity;
		// 头尾节点 item 为null next为null
        last = head = new Node<E>(null);
    }

    /**
     * Creates a {@code LinkedBlockingQueue} with a capacity of
     * {@link Integer#MAX_VALUE}, initially containing the elements of the
     * given collection,
     * added in traversal order of the collection's iterator.
     *
     * @param c the collection of elements to initially contain
     * @throws NullPointerException if the specified collection or any
     *         of its elements are null
     * 构建队列且将集合的元素入队
     */
    public LinkedBlockingQueue(Collection<? extends E> c) {
        this(Integer.MAX_VALUE);
		// 入队锁上锁
        final ReentrantLock putLock = this.putLock;
        putLock.lock(); // Never contended, but necessary for visibility
        try {
            int n = 0;
            for (E e : c) {
				// 不允许 null 元素 因为头结点的判定条件为 head.item == null
                if (e == null)
                    throw new NullPointerException();
                if (n == capacity)
                    throw new IllegalStateException("Queue full");
				// 构建Node节点入队
                enqueue(new Node<E>(e));
                ++n;
            }
			// 因为是在构造方法中不存在多线程竞争问题直接set
            count.set(n);
        } finally {
			// 释放锁
            putLock.unlock();
        }
    }

    // this doc comment is overridden to remove the reference to collections
    // greater in size than Integer.MAX_VALUE
    /**
     * Returns the number of elements in this queue.
     *
     * @return the number of elements in this queue
     * 返回当前队列中的元素个数
     */
    public int size() {
        return count.get();
    }

    // this doc comment is a modified copy of the inherited doc comment,
    // without the reference to unlimited queues.
    /**
     * Returns the number of additional elements that this queue can ideally
     * (in the absence of memory or resource constraints) accept without
     * blocking. This is always equal to the initial capacity of this queue
     * less the current {@code size} of this queue.
     *
     * <p>Note that you <em>cannot</em> always tell if an attempt to insert
     * an element will succeed by inspecting {@code remainingCapacity}
     * because it may be the case that another thread is about to
     * insert or remove an element.
     * 返回当前队列的剩余容量
     */
    public int remainingCapacity() {
        return capacity - count.get();
    }

    /**
     * Inserts the specified element at the tail of this queue, waiting if
     * necessary for space to become available.
     *
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
	 * 入队
     * 
     */
    public void put(E e) throws InterruptedException {
    	// 不允许null元素
        if (e == null) throw new NullPointerException();
        final int c;
		// 新new Node节点
        final Node<E> node = new Node<E>(e);
		// 使用put锁枷加锁
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
		// 上锁 允许中断
        putLock.lockInterruptibly();
        try {
            /*
             * Note that count is used in wait guard even though it is
             * not protected by lock. This works because count can
             * only decrease at this point (all other puts are shut
             * out by lock), and we (or some other waiting put) are
             * signalled if it ever changes from capacity. Similarly
             * for all other uses of count in other wait guards.
             * count 线程安全
             * 自旋等待 如果容量满了就阻塞 等待唤醒
             */
            while (count.get() == capacity) {
                notFull.await();
            }
			// 入队
            enqueue(node);
			// count 自增 线程安全
            c = count.getAndIncrement();
			// 如果队列没满  就唤醒一个阻塞在notFull条件上的线程(可以继续入队)
			// 这里为什么要唤醒一下呢？
        	// 因为可能有很多线程阻塞在notFull这个条件上的,而取元素时只有取之前队列是满的才会唤醒notFull,不用等到取元素时才唤醒
        	// 为什么队列满的才唤醒notFull呢？
        	// 因为唤醒是需要加putLock的，这是为了减少锁的次数,所以，这里索性在放完元素就检测一下，未满就唤醒其它notFull上的线程,说白了，这也是锁分离带来的代价
            if (c + 1 < capacity)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
		// c元put之前的元素个数,如果之前是0 有可能有线程阻塞,这里进行条件唤醒
        if (c == 0)
            signalNotEmpty();
    }

    /**
     * Inserts the specified element at the tail of this queue, waiting if
     * necessary up to the specified wait time for space to become available.
     *
     * @return {@code true} if successful, or {@code false} if
     *         the specified waiting time elapses before space is available
     * @throws InterruptedException {@inheritDoc}
     * @throws NullPointerException {@inheritDoc}
     * 入队 队列满的情况会阻塞指定时间
     */
    public boolean offer(E e, long timeout, TimeUnit unit)
        throws InterruptedException {

        if (e == null) throw new NullPointerException();
		// 转换时间
        long nanos = unit.toNanos(timeout);
        final int c;
		// 使用put锁上锁, 同样可以相应中断
        final ReentrantLock putLock = this.putLock;
        final AtomicInteger count = this.count;
        putLock.lockInterruptibly();
        try {
			// 容量满的话就阻塞指定时间
            while (count.get() == capacity) {
                if (nanos <= 0L)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
			// 入队
            enqueue(new Node<E>(e));
			// 同put一样计数器+1并且队列没满的话唤醒等待入队的线程
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
        return true;
    }

    /**
     * Inserts the specified element at the tail of this queue if it is
     * possible to do so immediately without exceeding the queue's capacity,
     * returning {@code true} upon success and {@code false} if this queue
     * is full.
     * When using a capacity-restricted queue, this method is generally
     * preferable to method {@link BlockingQueue#add add}, which can fail to
     * insert an element only by throwing an exception.
     *
     * @throws NullPointerException if the specified element is null
     * 入队，一直等待锁,队列满则失败 不会超时 逻辑同  offer(E e, long timeout, TimeUnit unit) 一样 区别就是直接获取put锁,获取失败将会一直阻塞
     */
    public boolean offer(E e) {
        if (e == null) throw new NullPointerException();
        final AtomicInteger count = this.count;
        if (count.get() == capacity)
            return false;
        final int c;
        final Node<E> node = new Node<E>(e);
        final ReentrantLock putLock = this.putLock;
        putLock.lock();
        try {
            if (count.get() == capacity)
                return false;
            enqueue(node);
            c = count.getAndIncrement();
            if (c + 1 < capacity)
                notFull.signal();
        } finally {
            putLock.unlock();
        }
        if (c == 0)
            signalNotEmpty();
        return true;
    }

	/**
	 * 出队
	 */
    public E take() throws InterruptedException {
        final E x;
        final int c;
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lockInterruptibly();
        try {
            while (count.get() == 0) {
                notEmpty.await();
            }
			// 出队
            x = dequeue();
			// 计数器 -1
            c = count.getAndDecrement();
			// 如果 c > 1 说明出队后队列不为空 唤醒其他等待出队的线程
            if (c > 1)
                notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
		// 如果 c == capacity 说明出队前队列已满,出队后不满,唤醒等待入队的线程
        if (c == capacity)
            signalNotFull();
        return x;
    }

	/**
	 * 出队,队列为空时将会等待指定时间
	 */
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        final E x;
        final int c;
		// 转换时间
        long nanos = unit.toNanos(timeout);
        final AtomicInteger count = this.count;
        final ReentrantLock takeLock = this.takeLock;
		// 上锁
        takeLock.lockInterruptibly();
        try {
			// 队列为空时阻塞
            while (count.get() == 0) {
                if (nanos <= 0L)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
			// 出队
            x = dequeue();
			// 同take基本相同		
            c = count.getAndDecrement();
            if (c > 1)
                notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
        if (c == capacity)
            signalNotFull();
        return x;
    }

	/**
	 * 出队
	 */
    public E poll() {
        final AtomicInteger count = this.count;
		// 队列为空直接返回null
        if (count.get() == 0)
            return null;
        final E x;
        final int c;
		// take锁上锁
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
			// 上锁后再次判断队列是否为空
            if (count.get() == 0)
                return null;
			// 出队
            x = dequeue();
            c = count.getAndDecrement();
			// 出队后不为空唤醒其他出队线程 跟入队时同理  入队时只有为空入队时才唤醒出队线程
            if (c > 1)
                notEmpty.signal();
        } finally {
            takeLock.unlock();
        }
		// 出队前队列满出队后则唤醒等待入队线程,因为这里都是只有队列满的时候出队才会唤醒,所以入队时不满也需要唤醒
        if (c == capacity)
            signalNotFull();
        return x;
    }

	/**
	 * 获取队首元素不出队
	 */
    public E peek() {
        final AtomicInteger count = this.count;
        if (count.get() == 0)
            return null;
        final ReentrantLock takeLock = this.takeLock;
		// 上锁
        takeLock.lock();
        try {
			// 上锁后再次判断元素个数
            return (count.get() > 0) ? head.next.item : null;
        } finally {
            takeLock.unlock();
        }
    }

    /**
     * Unlinks interior Node p with predecessor pred.
     * 从链表中移除节点p, pred为节点p的前驱节点
     */
    void unlink(Node<E> p, Node<E> pred) {
        // assert putLock.isHeldByCurrentThread();
        // assert takeLock.isHeldByCurrentThread();
        // p.next is not changed, to allow iterators that are
        // traversing p to maintain their weak-consistency guarantee.
        // 要移除,所以将item置为null 断开引用关系
        p.item = null;
		// 将pred.next指向p.next 将前驱节点的next指向p的下一个元素
        pred.next = p.next;
		// 如果p为尾节点,移除之后pred为尾节点
        if (last == p)
            last = pred;
		// 如果移除前队列满,移除后即不满则需要唤醒等待入队的线程
        if (count.getAndDecrement() == capacity)
            notFull.signal();
    }

    /**
     * Removes a single instance of the specified element from this queue,
     * if it is present.  More formally, removes an element {@code e} such
     * that {@code o.equals(e)}, if this queue contains one or more such
     * elements.
     * Returns {@code true} if this queue contained the specified element
     * (or equivalently, if this queue changed as a result of the call).
     *
     * @param o element to be removed from this queue, if present
     * @return {@code true} if this queue changed as a result of the call
     * 移除元素o
     */
    public boolean remove(Object o) {
        if (o == null) return false;
		// 因为移除的可能是队首元素,也可能是队尾元素,所以需要全局锁
        fullyLock();
        try {
			// 循环遍历
            for (Node<E> pred = head, p = pred.next;
                 p != null;
                 pred = p, p = p.next) {
                if (o.equals(p.item)) {
					// 移除节点p
                    unlink(p, pred);
                    return true;
                }
            }
            return false;
        } finally {
			// 释放锁
            fullyUnlock();
        }
    }

    /**
     * Returns {@code true} if this queue contains the specified element.
     * More formally, returns {@code true} if and only if this queue contains
     * at least one element {@code e} such that {@code o.equals(e)}.
     *
     * @param o object to be checked for containment in this queue
     * @return {@code true} if this queue contains the specified element
	 * 返回队列中是否存在元素o
     */
    public boolean contains(Object o) {
    	// 队列中不允许null元素,所以 == null直接返回false
        if (o == null) return false;
		// 全局锁 因为可能执行的时候元素o入队或者出队,所以需要全局锁
        fullyLock();
        try {
			// 遍历比较
            for (Node<E> p = head.next; p != null; p = p.next)
                if (o.equals(p.item))
                    return true;
            return false;
        } finally {
            fullyUnlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * proper sequence.
     *
     * <p>The returned array will be "safe" in that no references to it are
     * maintained by this queue.  (In other words, this method must allocate
     * a new array).  The caller is thus free to modify the returned array.
     *
     * <p>This method acts as bridge between array-based and collection-based
     * APIs.
     *
     * @return an array containing all of the elements in this queue
     * 转为数组
     */
    public Object[] toArray() {
    	// 同样为了保证当前元素不再增加减少需要全局锁
        fullyLock();
        try {
            int size = count.get();
            Object[] a = new Object[size];
            int k = 0;
            for (Node<E> p = head.next; p != null; p = p.next)
                a[k++] = p.item;
            return a;
        } finally {
            fullyUnlock();
        }
    }

    /**
     * Returns an array containing all of the elements in this queue, in
     * proper sequence; the runtime type of the returned array is that of
     * the specified array.  If the queue fits in the specified array, it
     * is returned therein.  Otherwise, a new array is allocated with the
     * runtime type of the specified array and the size of this queue.
     *
     * <p>If this queue fits in the specified array with room to spare
     * (i.e., the array has more elements than this queue), the element in
     * the array immediately following the end of the queue is set to
     * {@code null}.
     *
     * <p>Like the {@link #toArray()} method, this method acts as bridge between
     * array-based and collection-based APIs.  Further, this method allows
     * precise control over the runtime type of the output array, and may,
     * under certain circumstances, be used to save allocation costs.
     *
     * <p>Suppose {@code x} is a queue known to contain only strings.
     * The following code can be used to dump the queue into a newly
     * allocated array of {@code String}:
     *
     * <pre> {@code String[] y = x.toArray(new String[0]);}</pre>
     *
     * Note that {@code toArray(new Object[0])} is identical in function to
     * {@code toArray()}.
     *
     * @param a the array into which the elements of the queue are to
     *          be stored, if it is big enough; otherwise, a new array of the
     *          same runtime type is allocated for this purpose
     * @return an array containing all of the elements in this queue
     * @throws ArrayStoreException if the runtime type of the specified array
     *         is not a supertype of the runtime type of every element in
     *         this queue
     * @throws NullPointerException if the specified array is null
     */
    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
        fullyLock();
        try {
            int size = count.get();
            if (a.length < size)
                a = (T[])java.lang.reflect.Array.newInstance
                    (a.getClass().getComponentType(), size);

            int k = 0;
            for (Node<E> p = head.next; p != null; p = p.next)
                a[k++] = (T)p.item;
            if (a.length > k)
                a[k] = null;
            return a;
        } finally {
            fullyUnlock();
        }
    }

    public String toString() {
        return Helpers.collectionToString(this);
    }

    /**
     * Atomically removes all of the elements from this queue.
     * The queue will be empty after this call returns.
     * 清空队列
     */
    public void clear() {
    	// 同样使用全局锁
        fullyLock();
        try {
            for (Node<E> p, h = head; (p = h.next) != null; h = p) {
                h.next = h;
                p.item = null;
            }
            head = last;
            // assert head.item == null && head.next == null;
            if (count.getAndSet(0) == capacity)
                notFull.signal();
        } finally {
            fullyUnlock();
        }
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     */
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    /**
     * @throws UnsupportedOperationException {@inheritDoc}
     * @throws ClassCastException            {@inheritDoc}
     * @throws NullPointerException          {@inheritDoc}
     * @throws IllegalArgumentException      {@inheritDoc}
     */
    public int drainTo(Collection<? super E> c, int maxElements) {
        Objects.requireNonNull(c);
        if (c == this)
            throw new IllegalArgumentException();
        if (maxElements <= 0)
            return 0;
        boolean signalNotFull = false;
        final ReentrantLock takeLock = this.takeLock;
        takeLock.lock();
        try {
            int n = Math.min(maxElements, count.get());
            // count.get provides visibility to first n Nodes
            Node<E> h = head;
            int i = 0;
            try {
                while (i < n) {
                    Node<E> p = h.next;
                    c.add(p.item);
                    p.item = null;
                    h.next = h;
                    h = p;
                    ++i;
                }
                return n;
            } finally {
                // Restore invariants even if c.add() threw
                if (i > 0) {
                    // assert h.item == null;
                    head = h;
                    signalNotFull = (count.getAndAdd(-i) == capacity);
                }
            }
        } finally {
            takeLock.unlock();
            if (signalNotFull)
                signalNotFull();
        }
    }

    /**
     * Used for any element traversal that is not entirely under lock.
     * Such traversals must handle both:
     * - dequeued nodes (p.next == p)
     * - (possibly multiple) interior removed nodes (p.item == null)
     */
    Node<E> succ(Node<E> p) {
        if (p == (p = p.next))
            p = head.next;
        return p;
    }

    /**
     * Returns an iterator over the elements in this queue in proper sequence.
     * The elements will be returned in order from first (head) to last (tail).
     *
     * <p>The returned iterator is
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * @return an iterator over the elements in this queue in proper sequence
     */
    public Iterator<E> iterator() {
        return new Itr();
    }

    /**
     * Weakly-consistent iterator.
     *
     * Lazily updated ancestor field provides expected O(1) remove(),
     * but still O(n) in the worst case, whenever the saved ancestor
     * is concurrently deleted.
     */
    private class Itr implements Iterator<E> {
        private Node<E> next;           // Node holding nextItem
        private E nextItem;             // next item to hand out
        private Node<E> lastRet;
        private Node<E> ancestor;       // Helps unlink lastRet on remove()

        Itr() {
            fullyLock();
            try {
                if ((next = head.next) != null)
                    nextItem = next.item;
            } finally {
                fullyUnlock();
            }
        }

        public boolean hasNext() {
            return next != null;
        }

        public E next() {
            Node<E> p;
            if ((p = next) == null)
                throw new NoSuchElementException();
            lastRet = p;
            E x = nextItem;
            fullyLock();
            try {
                E e = null;
                for (p = p.next; p != null && (e = p.item) == null; )
                    p = succ(p);
                next = p;
                nextItem = e;
            } finally {
                fullyUnlock();
            }
            return x;
        }

        public void forEachRemaining(Consumer<? super E> action) {
            // A variant of forEachFrom
            Objects.requireNonNull(action);
            Node<E> p;
            if ((p = next) == null) return;
            lastRet = p;
            next = null;
            final int batchSize = 64;
            Object[] es = null;
            int n, len = 1;
            do {
                fullyLock();
                try {
                    if (es == null) {
                        p = p.next;
                        for (Node<E> q = p; q != null; q = succ(q))
                            if (q.item != null && ++len == batchSize)
                                break;
                        es = new Object[len];
                        es[0] = nextItem;
                        nextItem = null;
                        n = 1;
                    } else
                        n = 0;
                    for (; p != null && n < len; p = succ(p))
                        if ((es[n] = p.item) != null) {
                            lastRet = p;
                            n++;
                        }
                } finally {
                    fullyUnlock();
                }
                for (int i = 0; i < n; i++) {
                    @SuppressWarnings("unchecked") E e = (E) es[i];
                    action.accept(e);
                }
            } while (n > 0 && p != null);
        }

        public void remove() {
            Node<E> p = lastRet;
            if (p == null)
                throw new IllegalStateException();
            lastRet = null;
            fullyLock();
            try {
                if (p.item != null) {
                    if (ancestor == null)
                        ancestor = head;
                    ancestor = findPred(p, ancestor);
                    unlink(p, ancestor);
                }
            } finally {
                fullyUnlock();
            }
        }
    }

    /**
     * A customized variant of Spliterators.IteratorSpliterator.
     * Keep this class in sync with (very similar) LBDSpliterator.
     */
    private final class LBQSpliterator implements Spliterator<E> {
        static final int MAX_BATCH = 1 << 25;  // max batch array size;
        Node<E> current;    // current node; null until initialized
        int batch;          // batch size for splits
        boolean exhausted;  // true when no more nodes
        long est = size();  // size estimate

        LBQSpliterator() {}

        public long estimateSize() { return est; }

        public Spliterator<E> trySplit() {
            Node<E> h;
            if (!exhausted &&
                ((h = current) != null || (h = head.next) != null)
                && h.next != null) {
                int n = batch = Math.min(batch + 1, MAX_BATCH);
                Object[] a = new Object[n];
                int i = 0;
                Node<E> p = current;
                fullyLock();
                try {
                    if (p != null || (p = head.next) != null)
                        for (; p != null && i < n; p = succ(p))
                            if ((a[i] = p.item) != null)
                                i++;
                } finally {
                    fullyUnlock();
                }
                if ((current = p) == null) {
                    est = 0L;
                    exhausted = true;
                }
                else if ((est -= i) < 0L)
                    est = 0L;
                if (i > 0)
                    return Spliterators.spliterator
                        (a, 0, i, (Spliterator.ORDERED |
                                   Spliterator.NONNULL |
                                   Spliterator.CONCURRENT));
            }
            return null;
        }

        public boolean tryAdvance(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            if (!exhausted) {
                E e = null;
                fullyLock();
                try {
                    Node<E> p;
                    if ((p = current) != null || (p = head.next) != null)
                        do {
                            e = p.item;
                            p = succ(p);
                        } while (e == null && p != null);
                    if ((current = p) == null)
                        exhausted = true;
                } finally {
                    fullyUnlock();
                }
                if (e != null) {
                    action.accept(e);
                    return true;
                }
            }
            return false;
        }

        public void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action);
            if (!exhausted) {
                exhausted = true;
                Node<E> p = current;
                current = null;
                forEachFrom(action, p);
            }
        }

        public int characteristics() {
            return (Spliterator.ORDERED |
                    Spliterator.NONNULL |
                    Spliterator.CONCURRENT);
        }
    }

    /**
     * Returns a {@link Spliterator} over the elements in this queue.
     *
     * <p>The returned spliterator is
     * <a href="package-summary.html#Weakly"><i>weakly consistent</i></a>.
     *
     * <p>The {@code Spliterator} reports {@link Spliterator#CONCURRENT},
     * {@link Spliterator#ORDERED}, and {@link Spliterator#NONNULL}.
     *
     * @implNote
     * The {@code Spliterator} implements {@code trySplit} to permit limited
     * parallelism.
     *
     * @return a {@code Spliterator} over the elements in this queue
     * @since 1.8
     */
    public Spliterator<E> spliterator() {
        return new LBQSpliterator();
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     */
    public void forEach(Consumer<? super E> action) {
        Objects.requireNonNull(action);
        forEachFrom(action, null);
    }

    /**
     * Runs action on each element found during a traversal starting at p.
     * If p is null, traversal starts at head.
     */
    void forEachFrom(Consumer<? super E> action, Node<E> p) {
        // Extract batches of elements while holding the lock; then
        // run the action on the elements while not
        final int batchSize = 64;       // max number of elements per batch
        Object[] es = null;             // container for batch of elements
        int n, len = 0;
        do {
            fullyLock();
            try {
                if (es == null) {
                    if (p == null) p = head.next;
                    for (Node<E> q = p; q != null; q = succ(q))
                        if (q.item != null && ++len == batchSize)
                            break;
                    es = new Object[len];
                }
                for (n = 0; p != null && n < len; p = succ(p))
                    if ((es[n] = p.item) != null)
                        n++;
            } finally {
                fullyUnlock();
            }
            for (int i = 0; i < n; i++) {
                @SuppressWarnings("unchecked") E e = (E) es[i];
                action.accept(e);
            }
        } while (n > 0 && p != null);
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean removeIf(Predicate<? super E> filter) {
        Objects.requireNonNull(filter);
        return bulkRemove(filter);
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean removeAll(Collection<?> c) {
        Objects.requireNonNull(c);
        return bulkRemove(e -> c.contains(e));
    }

    /**
     * @throws NullPointerException {@inheritDoc}
     */
    public boolean retainAll(Collection<?> c) {
        Objects.requireNonNull(c);
        return bulkRemove(e -> !c.contains(e));
    }

    /**
     * Returns the predecessor of live node p, given a node that was
     * once a live ancestor of p (or head); allows unlinking of p.
     */
    Node<E> findPred(Node<E> p, Node<E> ancestor) {
        // assert p.item != null;
        if (ancestor.item == null)
            ancestor = head;
        // Fails with NPE if precondition not satisfied
        for (Node<E> q; (q = ancestor.next) != p; )
            ancestor = q;
        return ancestor;
    }

    /** Implementation of bulk remove methods. */
    @SuppressWarnings("unchecked")
    private boolean bulkRemove(Predicate<? super E> filter) {
        boolean removed = false;
        Node<E> p = null, ancestor = head;
        Node<E>[] nodes = null;
        int n, len = 0;
        do {
            // 1. Extract batch of up to 64 elements while holding the lock.
            fullyLock();
            try {
                if (nodes == null) {  // first batch; initialize
                    p = head.next;
                    for (Node<E> q = p; q != null; q = succ(q))
                        if (q.item != null && ++len == 64)
                            break;
                    nodes = (Node<E>[]) new Node<?>[len];
                }
                for (n = 0; p != null && n < len; p = succ(p))
                    nodes[n++] = p;
            } finally {
                fullyUnlock();
            }

            // 2. Run the filter on the elements while lock is free.
            long deathRow = 0L;       // "bitset" of size 64
            for (int i = 0; i < n; i++) {
                final E e;
                if ((e = nodes[i].item) != null && filter.test(e))
                    deathRow |= 1L << i;
            }

            // 3. Remove any filtered elements while holding the lock.
            if (deathRow != 0) {
                fullyLock();
                try {
                    for (int i = 0; i < n; i++) {
                        final Node<E> q;
                        if ((deathRow & (1L << i)) != 0L
                            && (q = nodes[i]).item != null) {
                            ancestor = findPred(q, ancestor);
                            unlink(q, ancestor);
                            removed = true;
                        }
                        nodes[i] = null; // help GC
                    }
                } finally {
                    fullyUnlock();
                }
            }
        } while (n > 0 && p != null);
        return removed;
    }

    /**
     * Saves this queue to a stream (that is, serializes it).
     *
     * @param s the stream
     * @throws java.io.IOException if an I/O error occurs
     * @serialData The capacity is emitted (int), followed by all of
     * its elements (each an {@code Object}) in the proper order,
     * followed by a null
     */
    private void writeObject(java.io.ObjectOutputStream s)
        throws java.io.IOException {

        fullyLock();
        try {
            // Write out any hidden stuff, plus capacity
            s.defaultWriteObject();

            // Write out all elements in the proper order.
            for (Node<E> p = head.next; p != null; p = p.next)
                s.writeObject(p.item);

            // Use trailing null as sentinel
            s.writeObject(null);
        } finally {
            fullyUnlock();
        }
    }

    /**
     * Reconstitutes this queue from a stream (that is, deserializes it).
     * @param s the stream
     * @throws ClassNotFoundException if the class of a serialized object
     *         could not be found
     * @throws java.io.IOException if an I/O error occurs
     */
    private void readObject(java.io.ObjectInputStream s)
        throws java.io.IOException, ClassNotFoundException {
        // Read in capacity, and any hidden stuff
        s.defaultReadObject();

        count.set(0);
        last = head = new Node<E>(null);

        // Read in all elements and place in queue
        for (;;) {
            @SuppressWarnings("unchecked")
            E item = (E)s.readObject();
            if (item == null)
                break;
            add(item);
        }
    }
}
