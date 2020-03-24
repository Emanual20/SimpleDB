package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private final Predicate now_p;
    private OpIterator now_child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        now_p=p;
        now_child=child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return now_p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return now_child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        now_child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        now_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        now_child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        while(now_child.hasNext()){
            Tuple temp_tuple=now_child.next();
            if(now_p.filter(temp_tuple)) return temp_tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {now_child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        now_child=children[0];
    }

}
