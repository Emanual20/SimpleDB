package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId t;
    private OpIterator child;

    private TupleDesc td;
    private int cnt_called;
    private boolean is_called;
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
        // some code goes here
        this.t=t;
        this.child=child;
        this.td=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{null});
        this.cnt_called=0;
        this.is_called=false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        child.open();
        super.open();
        this.cnt_called=0;
        this.is_called=false;
    }

    public void close() {
        // some code goes here
        this.is_called=true;
        this.cnt_called=0;
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
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
        // some code goes here
        if(is_called) return null;
        is_called=true;

        while(child.hasNext()){
            Tuple temp_tuple=child.next();
            try {
                Database.getBufferPool().deleteTuple(t, temp_tuple);
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
