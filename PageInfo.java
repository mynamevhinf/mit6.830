package simpledb;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

class PageInfo {
    Page page;
    int nReader, nWriter;
    PageInfo prev, next;
    ReentrantLock lock;
    Condition condition;
    TransactionId exTransaction;
    ConcurrentHashMap<TransactionId, Integer> shareLockSet;
    //private boolean canReclaim = false;
    //private boolean willReclaim = false;

    //public boolean isWillReclaim() { return willReclaim; }
    public TransactionId getOnwner() { return exTransaction; }

    //public boolean tryLockMeta() { return lock.tryLock(); }
    //public void unLockMeta() { lock.unlock(); }

    public boolean isDirty() { return page.isDirty() != null; }

    public boolean isHoldingPage(TransactionId tid)
    {
        boolean flag;
        lock.lock();
        flag = tid.equals(exTransaction) || shareLockSet.contains(tid);
        lock.unlock();
        return flag;
    }

    private void acquireShareLock(TransactionId tid)
    {
        lock.lock();

        /// if we have acquire ex-lock
        if (exTransaction == tid) {
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
        --nReader;
        //if (--nReader == 0)
            //canReclaim = true;
        Integer cnt = shareLockSet.get(tid);
        if (cnt == null)
            return;
        if (cnt == 0) {
            System.out.println("cannot unlock a lock u don't hold it!");
            System.exit(-1);
        }
        shareLockSet.put(tid, cnt-1);
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
        if (exTransaction == tid) {
            --nWriter;
            //if (--nWriter == 0)
            //    canReclaim = true;
            exTransaction = null;
        } else
            releaseShareLock(tid);
        condition.signalAll();
        lock.unlock();
    }

    void acquireLock(TransactionId tid, Permissions permissions)
    {
        if (permissions == Permissions.READ_ONLY)
            acquireShareLock(tid);
        else
            acquireExLock(tid);
    }

    static PageInfo newPageInfo(Page page) //Permissions permissions, TransactionId transactionId)
    {
        PageInfo pageInfo = new PageInfo();
        pageInfo.page = page;
        pageInfo.prev = pageInfo.next = pageInfo;
        pageInfo.shareLockSet = new ConcurrentHashMap<>();
        pageInfo.lock = new ReentrantLock();
        pageInfo.nReader = pageInfo.nWriter = 0;
        pageInfo.condition = pageInfo.lock.newCondition();
        return pageInfo;
    }
}
