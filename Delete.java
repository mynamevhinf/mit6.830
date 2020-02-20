package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    OpIterator[] childs;
    TransactionId transactionId;

    int cnt;
    TupleDesc td;
    Tuple resTuple;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        cnt = 0;
        transactionId = t;
        childs = new OpIterator[] { child };
        td = new TupleDesc(new Type[]{ Type.INT_TYPE }, null);
    }

    public TupleDesc getTupleDesc() {
        return childs[0].getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        OpIterator opIterator = childs[0];
        opIterator.open();
        BufferPool bufferPool = Database.getBufferPool();

        while (opIterator.hasNext()) {
            Tuple t = opIterator.next();

            try {
                bufferPool.deleteTuple(transactionId, t);
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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
