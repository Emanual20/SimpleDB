package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */

    private final int now_field1;
    private final Predicate.Op now_op;
    private final int now_field2;

    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // some code goes here
        now_field1=field1;
        now_op=op;
        now_field2=field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // some code goes here
        //System.out.println(1);
        return t1.getField(now_field1).compare(now_op,t2.getField(now_field2));
    }
    
    public int getField1()
    {
        // some code goes here
        return now_field1;
    }
    
    public int getField2()
    {
        // some code goes here
        return now_field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return now_op;
    }
}
