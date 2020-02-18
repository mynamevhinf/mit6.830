package simpledb;

import javax.xml.crypto.Data;
import java.awt.image.DataBuffer;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
    File f;
    RandomAccessFile rf;
    TupleDesc tupleDesc;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        try {
            this.rf = new RandomAccessFile(f, "rw");
        } catch (FileNotFoundException e) {
            System.out.println("Failed at create HeapFile!");
            System.exit(-1);
        }
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageNo = pid.getPageNumber();
        if (pageNo < 0 || pageNo > numPages())
            throw new IllegalArgumentException();
        try {
            int pageSize = BufferPool.getPageSize();
            byte[] data = new byte[pageSize];
            rf.seek(pageNo * pageSize);
            rf.read(data);
            //fileInputStream.read(data, pageNo*pageSize, pageSize);
            return new HeapPage((HeapPageId) pid, data);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int len = (int) f.length();
        int pageSize = BufferPool.getPageSize();
        return (len / pageSize) + (len % pageSize != 0 ? 1 : 0);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return HeapFileScanIterator.newHeapFileScanIterator(tid, this);
    }

}

class HeapFileScanIterator extends AbstractDbFileIterator {
    HeapFile hf;
    int currPgNo;
    TransactionId tid;
    Iterator<Tuple> tupleIterator;

    static HeapFileScanIterator newHeapFileScanIterator(TransactionId tid, HeapFile file)
    {
        HeapFileScanIterator iterator = new HeapFileScanIterator();
        iterator.hf = file;
        iterator.tid = tid;
        iterator.currPgNo = -1;
        iterator.tupleIterator = null;
        return iterator;
    }

    @Override
    public Tuple readNext() throws DbException, TransactionAbortedException {
        if (currPgNo == -1) // not yet opened
            return null;
        if (tupleIterator == null) // has closed
            throw new NoSuchElementException();
        if (tupleIterator.hasNext())
            return tupleIterator.next();
        if (++currPgNo >= hf.numPages())
            return null;
        HeapPageId pageId = new HeapPageId(hf.getId(), currPgNo);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        tupleIterator = page.iterator();
        return tupleIterator.next();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        currPgNo = 0;
        HeapPageId pageId = new HeapPageId(hf.getId(), 0);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        tupleIterator = page.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        open();
    }

    @Override
    public void close() {
        super.close();
        tupleIterator = null;
    }
}
