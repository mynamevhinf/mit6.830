package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /// used for LRU...
    int size, numPages;
    private PageInfo head;
    ConcurrentHashMap<PageId, PageInfo> pagesMap;

    Condition condition;
    ReentrantLock bufferLock;
    DeadLockManager deadLockManager;

    HashSet<TransactionId> activeTransactions = new HashSet<>();

    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        //head = tail = null;
        size = 0;
        this.numPages = numPages;
        head = PageInfo.newPageInfo(null, null);   /// guard
        pagesMap = new ConcurrentHashMap<>();
        bufferLock = new ReentrantLock();
        condition = bufferLock.newCondition();

        deadLockManager = DeadLockManager.newDeadLockManager(this);
        deadLockManager.start();
    }
    
    public static int getPageSize() {
        return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    boolean tryLockBufferPool() { return bufferLock.tryLock(); }
    void LockBufferPool() { bufferLock.lock(); }
    void unLockBufferPool() { bufferLock.unlock(); }

    public ConcurrentHashMap<PageId, PageInfo> getPagesMap() {
        return pagesMap;
    }

    private void acquireLock(PageInfo pageInfo, TransactionId tid, Permissions permissions)
    {
        deadLockManager.tryLock(tid, pageInfo.getPageId(), permissions);
        pageInfo.acquireLock(tid, permissions);
        deadLockManager.getLock(tid);
    }

    private void releaseLock(PageInfo pageInfo, TransactionId tid)
    {
        pageInfo.releaseLock(tid);
    }

    boolean hasActiveTransactions() { return activeTransactions.size() != 0; }

    public PageInfo getRealPageInfo(TransactionId tid, PageInfo pageInfo, PageId pageId, Permissions permissions) throws DbException {
        bufferLock.lock();
        activeTransactions.add(tid);
        while (true) {
            PageInfo existed = pagesMap.get(pageId);
            /// what will happned if someone else has been insert the page before we get writeLock...
            if (existed != null) {
                bufferLock.unlock();
                acquireLock(existed, tid, permissions);

                /// it has been discard... because previous transaction abort!!!
                bufferLock.lock();
                PageInfo other = pagesMap.get(pageId);
                if (other == null || other.getTimeStamp() != existed.getTimeStamp())
                    continue;
                accessPageInfo(other);
                bufferLock.unlock();
                return other;
            }

            pagesMap.put(pageId, pageInfo);
            evictPage();
            //waitUntilPoolNotFull();
            insertPageInfoToHead(pageInfo);
            bufferLock.unlock();
            return pageInfo;
        }
    }

    public void insertPageInfoToHead(PageInfo pageInfo)
    {
        pageInfo.next = head;
        pageInfo.prev = head.prev;
        head.prev.next = pageInfo;
        head.prev = pageInfo;
        size++;
    }

    private void accessPageInfo(PageInfo pageInfo) {
        if (pageInfo.isInList()) {
            pageInfo.prev.next = pageInfo.next;
            pageInfo.next.prev = pageInfo.prev;
            pageInfo.prev = pageInfo.next = pageInfo;
        }
        pageInfo.next = head;
        pageInfo.prev = head.prev;
        head.prev.next = pageInfo;
        head.prev = pageInfo;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        PageInfo pageInfo = pagesMap.get(pid);
        if (pageInfo == null) {
            pageInfo = PageInfo.newPageInfo(null, pid);
            acquireLock(pageInfo, tid, perm);
            pageInfo = getRealPageInfo(tid, pageInfo, pid, perm);
            if (!pageInfo.hasContent()) {
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                pageInfo.setPage(page);
            }
            //System.out.println("add new: " + pageInfo + ", pid " + pid.getPageNumber() + ", " + pageInfo.page.hashCode());
            return pageInfo.page;
        }

        acquireLock(pageInfo, tid, perm);
        /// after we get the lock, it may has been evit
        /// out form buffer...
        pageInfo = getRealPageInfo(tid, pageInfo, pid, perm);
        if (!pageInfo.hasContent()) {
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageInfo.setPage(page);
        }
        return pageInfo.page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        PageInfo pageInfo = pagesMap.get(pid);
        if (pageInfo != null)
            releaseLock(pageInfo, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        boolean flag = false;
        PageInfo pageInfo = pagesMap.get(p);
        if (pageInfo != null)
            flag = pageInfo.isHoldingPage(tid);
        return flag;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        bufferLock.lock();
        for (Map.Entry<PageId, PageInfo> entry : pagesMap.entrySet()) {
            PageId pageId = entry.getKey();
            PageInfo pageInfo = entry.getValue();
            if (pageInfo.isDirty() && tid.equals(pageInfo.getOnwner())) {
                if (commit) {
                    pageInfo.page.markDirty(false, null);
                    DbFile f = Database.getCatalog().getDatabaseFile(pageId.getTableId());
                    f.writePage(pageInfo.page);
                } else
                    pageInfo.giveUpContent();
                    //discardPage(pageId);
            }
            releasePage(tid, pageId);
        }
        activeTransactions.remove(tid);
        bufferLock.unlock();
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = f.insertTuple(tid, t);
        pages.get(0).markDirty(true, tid);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = f.deleteTuple(tid, t);
        pages.get(0).markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        bufferLock.lock();
        Collection<PageInfo> pages = pagesMap.values();
        for (PageInfo pageInfo : pages)
            flushPage(pageInfo.page.getId());
        bufferLock.unlock();
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public void discardPage(PageId pid) {
        pagesMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        PageInfo pageInfo = pagesMap.get(pid);
        Page page = pageInfo.page;
        if (page.isDirty() != null) {
            int tableId = pid.getTableId();
            DbFile f = Database.getCatalog().getDatabaseFile(tableId);
            f.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        bufferLock.lock();
        for (Map.Entry<PageId, PageInfo> entry : pagesMap.entrySet()) {
            PageId pageId = entry.getKey();
            PageInfo pageInfo = entry.getValue();
            if (tid.equals(pageInfo.getOnwner()))
                flushPage(pageId);
        }
        bufferLock.unlock();
    }


    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private void evictPage() throws DbException {
        if (size < numPages)
            return;

        PageInfo prev = head;
        PageInfo removed = prev.next;
        while (removed != head && removed.isDirty()) {
            prev = removed;
            removed = removed.next;
        }

        if (removed == head)
            throw new DbException("trying to reclaim old page when all pages in buffer was using!");

        prev.next = removed.next;
        prev.next.prev = prev;
        removed.next = removed.prev = removed;
        size--;

        if (removed.canReclaim())
            discardPage(removed.page.getId());
        else
            removed.giveUpContent();
        condition.signal();
    }
}
