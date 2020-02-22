package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

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
    ReentrantReadWriteLock rwLock;

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
        head = PageInfo.newPageInfo(null);   /// guard
        pagesMap = new ConcurrentHashMap<>();
        rwLock = new ReentrantReadWriteLock();
        condition = rwLock.writeLock().newCondition();
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

    /// we must hold lock before calling the function!!!
    public void waitUntilPoolNotFull() throws DbException {
        while (size == numPages) {
            System.out.println("size = " + size + ", numPages = " + numPages);
            evictPage();
            if (size != numPages)
                break;

            try {
                condition.await();
            } catch (InterruptedException e) {
                continue;
            }
        }
    }

    private void OptionInsertPage(TransactionId tid, Page page, Permissions permissions) throws DbException {
        rwLock.readLock().lock();
        PageInfo pageInfo = pagesMap.get(page.getId());
        rwLock.readLock().unlock();
        if (pageInfo == null) {
            pageInfo = PageInfo.newPageInfo(page);
            pageInfo.acquireLock(tid, permissions);
            insertPageInfoAndPage(tid, pageInfo, page.getId(), permissions);
            return;
        }

        //accessPageInfo(pageInfo);
    }

    private void OptionInsertPageInfo(TransactionId tid, PageInfo pageInfo, Permissions permissions) throws DbException {
        insertPageInfoAndPage(tid, pageInfo, pageInfo.page.getId(), permissions);
    }

    /// every pages must be locked before inserted!!!
    public void insertNewPageInfo(PageInfo pageInfo) throws DbException {
        waitUntilPoolNotFull();
        insertPageInfoToHead(pageInfo);
    }

    public void insertPageInfoAndPage(TransactionId tid, PageInfo pageInfo, PageId pageId, Permissions permissions) throws DbException {
        rwLock.writeLock().lock();
        PageInfo existed = pagesMap.get(pageId);
        /// what will happned if someone else has been insert the page before we get writeLock...
        if (existed != null) {
            /// locked it in bufferPool...
            existed.cannotReclaim();
            accessPageInfo(existed);
            condition.signalAll();
            rwLock.writeLock().unlock();
            existed.acquireLock(tid, permissions);
            return ;
        }

        pagesMap.put(pageId, pageInfo);
        insertNewPageInfo(pageInfo);

        condition.signalAll();
        rwLock.writeLock().unlock();
        //rwLock.notifyAll();
    }


    private PageInfo reclaimOldPage() throws DbException {
        PageInfo prev = head;
        PageInfo removed = prev.next;
        while (removed != head && !removed.canReclaim()) {
            prev = removed;
            removed = removed.next;
        }

        if (removed == head)
            throw new DbException("trying to reclaim old page when bufferPool is empty or all pages in buffer was using!");
        prev.next = removed.next;
        prev.next.prev = prev;
        size--;
        return removed;
    }

    public void insertPageInfoToHead(PageInfo pageInfo)
    {
        pageInfo.next = head;
        pageInfo.prev = head.prev;
        head.prev.next = pageInfo;
        head.prev = pageInfo;
        size++;
    }

    private void deletePageInfo(PageInfo pageInfo)
    {
        pageInfo.prev.next = pageInfo.next;
        pageInfo.next.prev = pageInfo.prev;
        pageInfo.prev = pageInfo.next = pageInfo;
        size--;
    }

    private void accessPageInfo(PageInfo pageInfo) {
        deletePageInfo(pageInfo);
        insertPageInfoToHead(pageInfo);
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
        rwLock.readLock().lock();
        PageInfo pageInfo = pagesMap.get(pid);
        rwLock.readLock().unlock();

        if (pageInfo == null) {
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageInfo = PageInfo.newPageInfo(page);
            pageInfo.acquireLock(tid, perm);

            insertPageInfoAndPage(tid, pageInfo, pid, perm);
            return page;
        }

        pageInfo.acquireLock(tid, perm);
        /// after we get the lock, it may has been evit
        /// out form buffer...
        OptionInsertPageInfo(tid, pageInfo, perm);
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
        rwLock.readLock().lock();
        PageInfo pageInfo = pagesMap.get(pid);
        rwLock.readLock().unlock();
        if (pageInfo != null)
            pageInfo.releaseLock(tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        boolean flag = false;
        rwLock.readLock().lock();
        PageInfo pageInfo = pagesMap.get(p);
        rwLock.readLock().unlock();

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
        // some code goes here
        // not necessary for lab1|lab2
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
        for (Page page : pages) {
            page.markDirty(true, tid);

            rwLock.readLock().lock();
            PageInfo pageInfo = pagesMap.get(page.getId());
            rwLock.readLock().unlock();

            pageInfo.releaseExLock(tid);
            OptionInsertPage(tid, page, Permissions.READ_WRITE);
        }
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
        PageId pageId = t.getRecordId().getPageId();
        int tableId = pageId.getTableId();
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> pages = f.deleteTuple(tid, t);
        for (Page page : pages) {
            page.markDirty(true, tid);

            rwLock.readLock().lock();
            PageInfo pageInfo = pagesMap.get(page.getId());
            rwLock.readLock().unlock();

            pageInfo.releaseExLock(tid);
            OptionInsertPage(tid, page, Permissions.READ_WRITE);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        rwLock.writeLock().lock();
        Collection<PageInfo> pages = pagesMap.values();
        for (PageInfo pageInfo : pages)
            flushPage(pageInfo.page.getId());
        rwLock.writeLock().unlock();
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public void discardPage(PageId pid) {
        rwLock.writeLock().lock();
        PageInfo pageInfo = pagesMap.get(pid);
        pagesMap.remove(pid);
        rwLock.writeLock().unlock();
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
        rwLock.writeLock().lock();
        for (Map.Entry<PageId, PageInfo> entry : pagesMap.entrySet()) {
            PageId pageId = entry.getKey();
            PageInfo pageInfo = entry.getValue();
            if (tid.equals(pageInfo.getOnwner()))
                flushPage(pageId);
        }
        rwLock.writeLock().unlock();
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private void evictPage() throws DbException {
        PageInfo pageInfo = reclaimOldPage();

        try {
            flushPage(pageInfo.page.getId());
            pagesMap.remove(pageInfo.page.getId());
        } catch (IOException e) {
            throw new DbException("IOException occurs when flusing dirty page in evictPage: " + e.getMessage());
        }
        rwLock.writeLock().unlock();
    }

}
