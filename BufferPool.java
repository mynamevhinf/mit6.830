package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

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
    static class PageInfo {
        Page page;
        //Permissions permissions;
        //TransactionId transactionId;
        PageInfo prev, next;

        static PageInfo newPageInfo(Page page) //Permissions permissions, TransactionId transactionId)
        {
            PageInfo pageInfo = new PageInfo();
            pageInfo.page = page;
            pageInfo.prev = pageInfo.next = pageInfo;
            return pageInfo;
        }
    }

    /// used for LRU...
    int size, numPages;
    private PageInfo head;
    ConcurrentHashMap<PageId, PageInfo> pagesMap;

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

    public void checkIfPoolFulled() throws DbException {
        if (size == numPages)
            evictPage();
    }

    private void OptionInsertPage(Page page) throws DbException {
        PageInfo pageInfo = pagesMap.get(page.getId());
        if (pageInfo == null) {
            pageInfo = PageInfo.newPageInfo(page);
            insertNewPageInfo(pageInfo);
            pagesMap.put(page.getId(), pageInfo);
        }
        accessPageInfo(pageInfo);
    }

    public void insertNewPageInfo(PageInfo pageInfo) throws DbException {
        checkIfPoolFulled();
        pageInfo.next = head;
        pageInfo.prev = head.prev;
        head.prev.next = pageInfo;
        head.prev = pageInfo;
        size++;
    }

    private PageInfo reclaimOldPage() throws DbException {
        if (head.next == head)
            throw new DbException("trying to reclaim old page when bufferPool is empty!");
        PageInfo removed = head.next;
        head.next = removed.next;
        removed.next.prev = head;
        size--;
        return removed;
    }

    private void deletePageInfo(PageInfo pageInfo)
    {
        pageInfo.prev.next = pageInfo.next;
        pageInfo.next.prev = pageInfo.prev;
        pageInfo.prev = pageInfo.next = pageInfo;
        size--;
    }

    private void accessPage(PageId pageId) throws DbException
    {
        accessPageInfo(pagesMap.get(pageId));
    }

    private void accessPageInfo(PageInfo pageInfo) throws DbException {
        deletePageInfo(pageInfo);
        insertNewPageInfo(pageInfo);
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
            //System.out.println("no pageInfo...");
            Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageInfo = PageInfo.newPageInfo(page);
            insertNewPageInfo(pageInfo);
            pagesMap.put(pid, pageInfo);
            return page;
        }
        accessPageInfo(pageInfo);
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
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
        // some code goes here
        // not necessary for lab1|lab2
        return false;
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
            OptionInsertPage(page);
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
            OptionInsertPage(page);
        }
        // accessPage(pageId);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        Collection<PageInfo> pages = pagesMap.values();
        for (PageInfo pageInfo : pages)
            flushPage(pageInfo.page.getId());
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        PageInfo pageInfo = pagesMap.get(pid);
        try {
            accessPageInfo(pageInfo);
        } catch (DbException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        pagesMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        PageInfo pageInfo = pagesMap.get(pid);
        Page page = pageInfo.page;
        if (page.isDirty() != null) {
            int tableId = page.getId().getTableId();
            DbFile f = Database.getCatalog().getDatabaseFile(tableId);
            f.writePage(page);
            page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        PageInfo pageInfo = reclaimOldPage();
        try {
            flushPage(pageInfo.page.getId());
            pagesMap.remove(pageInfo.page.getId());
        } catch (IOException e) {
            throw new DbException("IOException occurs when flusing dirty page in evictPage: " + e.getMessage());
        }
    }

}
