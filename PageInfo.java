package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class PageInfo {
    private static AtomicLong TIMESTAMP = new AtomicLong(0);

    private long timeStamp = TIMESTAMP.getAndIncrement();

    Page page;
    PageId pid;
    int nReader, nWriter;
    PageInfo prev, next;
    ReentrantLock lock;
    Condition condition;
    TransactionId exTransaction;
    ConcurrentHashMap<TransactionId, Integer> shareLockSet;

    public PageId getPageId() {
        return pid;
    }

    public ConcurrentHashMap<TransactionId, Integer> getShareLockSet() {
        return shareLockSet;
    }

    public TransactionId getExTransaction() {
        return exTransaction;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null || !(obj instanceof PageInfo))
            return false;
        PageInfo other = (PageInfo) obj;
        return timeStamp == other.getTimeStamp();
    }


    //public boolean isWillReclaim() { return willReclaim; }
    public TransactionId getOnwner() { return exTransaction; }

    public long getTimeStamp() {
        return timeStamp;
    }

    //public boolean tryLockMeta() { return lock.tryLock(); }
    //public void unLockMeta() { lock.unlock(); }

    public boolean isDirty() { return page == null || page.isDirty() != null; }

    public boolean isHoldingPage(TransactionId tid)
    {
        lock.lock();
        boolean flag = tid.equals(exTransaction) || shareLockSet.contains(tid);
        lock.unlock();
        return flag;
    }

    private void acquireShareLock(TransactionId tid)
    {
        lock.lock();

        /// if we have hold ex-lock
        if (tid.equals(exTransaction)) {
            lock.unlock();
            return;
        }

        while (nWriter != 0) {
            try {
                condition.await();
            } catch (InterruptedException e) {
                continue;
            }
        }

        nReader++;
        //canReclaim = false;
        lock.unlock();
        Integer cnt = shareLockSet.get(tid);
        shareLockSet.put(tid, cnt == null ? 1 : cnt + 1);
    }

    void releaseShareLock(TransactionId tid)
    {
        //shareLockSet.remove(tid);
        //if (--nReader == 0)
            //canReclaim = true;
        Integer cnt = shareLockSet.get(tid);
        if (cnt == null)
            return;
        if (cnt == 0) {
            System.out.println("cannot unlock a lock u don't hold it!");
            cnt /= 0;
            //System.exit(-1);
        }
        nReader -= cnt;
        shareLockSet.remove(tid);
        //shareLockSet.put(tid, cnt-1);
    }

    private void tryRemoveShareLock(TransactionId tid)
    {
        Integer cnt = shareLockSet.get(tid);
        if (cnt != null) {
            nReader -= cnt;
            shareLockSet.remove(tid);
        }
    }

    private void acquireExLock(TransactionId tid)
    {
        lock.lock();
        if (tid.equals(exTransaction)) {
            lock.unlock();
            return;
        }

        nWriter++;
        tryRemoveShareLock(tid);
        while (nReader != 0 && exTransaction != null) {
            try {
                condition.await();//lock.wait();
            } catch (InterruptedException e) {
                continue;
            }
        }
        //canReclaim = false;
        exTransaction = tid;
        lock.unlock();
    }

    void releaseExLock(TransactionId tid)
    {
        lock.lock();

        if (exTransaction != tid) {
            lock.unlock();
            return;
        }

        --nWriter;
        //if (--nWriter == 0)
        //    canReclaim = true;
        exTransaction = null;
        condition.signalAll();
        lock.unlock();
    }

    void releaseLock(TransactionId tid)
    {
        lock.lock();
        if (tid.equals(exTransaction)) {
            --nWriter;
            exTransaction = null;
        } else
            releaseShareLock(tid);
        condition.signalAll();
        lock.unlock();
    }

    void acquireLock(TransactionId tid, Permissions permissions)
    {
        if (Permissions.READ_ONLY.equals(permissions))
            acquireShareLock(tid);
        else
            acquireExLock(tid);
    }

    void giveUpContent() {
        page = null;
    }

    boolean canReclaim() {
        return nReader + nWriter == 0;
    }

    boolean hasContent() { return page != null; }

    public void setPage(Page page) {
        this.page = page;
        this.pid = page.getId();
    }

    boolean isInList() { return next != prev; }

    static PageInfo newPageInfo(Page page, PageId pageId) //Permissions permissions, TransactionId transactionId)
    {
        PageInfo pageInfo = new PageInfo();
        pageInfo.page = page;
        pageInfo.pid = pageId;
        pageInfo.prev = pageInfo.next = pageInfo;
        pageInfo.shareLockSet = new ConcurrentHashMap<>();
        pageInfo.lock = new ReentrantLock();
        pageInfo.nReader = pageInfo.nWriter = 0;
        pageInfo.condition = pageInfo.lock.newCondition();
        return pageInfo;
    }
}
