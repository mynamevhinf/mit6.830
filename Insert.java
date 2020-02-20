package simpledb;

import com.sun.corba.se.impl.orb.DataCollectorBase;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    int cnt, tableId;
    OpIterator[] childs;
    TransactionId transactionId;

    TupleDesc td;
    Tuple resTuple;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {

        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("Try to insert tuples into a mismatching table!");

        transactionId = t;
        cnt = 0;
        this.tableId = tableId;
        childs = new OpIterator[] { child };
        td = new TupleDesc(new Type[]{ Type.INT_TYPE }, null);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        childs[0].open();

        OpIterator opIterator = childs[0];
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        while (opIterator.hasNext()) {
            Tuple t = opIterator.next();
            try {
                f.insertTuple(transactionId, t);
            } catch (IOException e) {
                e.printStackTrace();
                System.exit(-1);
            }
            cnt++;
        }
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(cnt));
        resTuple = t;
    }

    public void close() {
        super.close();
        childs[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        cnt = 0;
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        Tuple t = resTuple;
        resTuple = null;
        return t;
    }

    @Override
    public OpIterator[] getChildren() {
        return childs;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        childs = children;
    }
}
