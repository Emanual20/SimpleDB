package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId t;
    private OpIterator child;
    private final int tableId;
    private final TupleDesc td;

    private boolean is_called;
    private int cnt_called;
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
        // some code goes here
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("TupleDesc doesn't match");

        this.t=t;
        this.child=child;
        this.tableId=tableId;
        this.td=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{null});
        this.is_called=false;
        this.cnt_called=0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        is_called=false;
        cnt_called=0;
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        is_called=false;
        cnt_called=0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
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
        // some code goes here
        if(is_called) return null;
        is_called=true;
        while(child.hasNext()){
            Tuple temp_tuple=child.next();
            try {
                Database.getBufferPool().insertTuple(t, tableId, temp_tuple);
                cnt_called++;
            }
            catch(IOException ioe_exception){
                ioe_exception.printStackTrace();
                break;
            }
        }
        Tuple ret_tuple=new Tuple(td);
        ret_tuple.setField(0,new IntField(cnt_called));
        return ret_tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child=children[0];
    }
}
