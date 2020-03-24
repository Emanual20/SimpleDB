package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    private final JoinPredicate now_p;
    private OpIterator now_child1,now_child2;
    private Tuple now_tp1;

    /**
     * Constructor. Accepts two children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, OpIterator child1, OpIterator child2) {
        // some code goes here
        now_p=p;
        now_child1=child1;
        now_child2=child2;
        now_tp1=null;
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return now_p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return now_child1.getTupleDesc().getFieldName(now_p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return now_child2.getTupleDesc().getFieldName(now_p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TupleDesc.merge(now_child1.getTupleDesc(),now_child2.getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        now_child1.open();
        now_child2.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        now_child2.close();
        now_child1.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        now_child1.rewind();
        now_child2.rewind();
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(!now_child1.hasNext()&&now_tp1==null) return null;
        while(now_child1.hasNext()||now_tp1!=null){
            if(now_child1.hasNext() && now_tp1==null) now_tp1=now_child1.next();
            while(now_child2.hasNext()){
                Tuple tp2=now_child2.next();
                if(now_p.filter(now_tp1,tp2)){
                    TupleDesc temp_td=TupleDesc.merge(now_tp1.getTupleDesc(),tp2.getTupleDesc());
                    Tuple tp_ret=new Tuple(temp_td);
                    for(int i=0;i<now_tp1.getTupleDesc().numFields();i++){
                        tp_ret.setField(i,now_tp1.getField(i));
                    }
                    for(int i=0;i<tp2.getTupleDesc().numFields();i++){
                        tp_ret.setField(now_tp1.getTupleDesc().numFields()+i,tp2.getField(i));
                    }
                    /*
                    for(int i=0;i<now_tp1.getTupleDesc().numFields();i++){
                        System.out.println(now_tp1.getField(i));
                    }
                    System.out.println(" ");
                    for(int i=0;i<tp2.getTupleDesc().numFields();i++){
                        System.out.println(tp2.getField(i));
                    }
                    System.out.println(" ");
                    for(int i=0;i<tp_ret.getTupleDesc().numFields();i++){
                        System.out.println(tp_ret.getField(i));
                    }
                    System.out.println(" ");
                    */
                    return tp_ret;
                }
            }
            now_child2.rewind();
            now_tp1=null;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {now_child1,now_child2};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        now_child1=children[0];
        now_child2=children[1];
    }

}
