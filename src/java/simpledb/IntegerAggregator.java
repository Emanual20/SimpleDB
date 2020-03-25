package simpledb;

import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbgieldtype;
    private final int afield;
    private final Op what;
    private TupleDesc td_rem;
    private ConcurrentHashMap<Field,items> map_gbfield2result;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbgieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        map_gbfield2result=new ConcurrentHashMap<>();
    }

    public class items{
        public int val;
        public int num_average;
        public items(int val,int num_average){
            this.val=val; this.num_average=num_average;
        }
        public items(int val){
            this.val=val; this.num_average=0;
        }
        public items(){
            this.val=0; this.num_average=0;
        }
    }
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup)
            throws UnsupportedOperationException{
        // some code goes here
        Field hash_gbfield=tup.getField(gbfield);
        IntField field_to_aggregate=(IntField) tup.getField(afield);
        int num_to_aggregate=field_to_aggregate.getValue();
        td_rem=tup.getTupleDesc();

        if(tup.getField(afield).getType()!=Type.INT_TYPE)
            throw new UnsupportedOperationException("the type of afield is not INT_TYPE");

        if(!map_gbfield2result.containsKey(hash_gbfield)){
            items items_to_insert=new items(num_to_aggregate,0);
            if(what==Op.AVG){
                items_to_insert.num_average=1;
            }
            else if(what==Op.COUNT){
                items_to_insert.val=1;
            }
            map_gbfield2result.put(hash_gbfield,items_to_insert);
        }
        else{
            items item_to_update=map_gbfield2result.get(hash_gbfield);
            if(what==Op.AVG){
                item_to_update.val=(item_to_update.val*item_to_update.num_average+num_to_aggregate)/(item_to_update.num_average+1);
                item_to_update.num_average++;
            }
            else{
                item_to_update.val=OldValue2NewValue(item_to_update.val,num_to_aggregate,what);
            }
            map_gbfield2result.replace(hash_gbfield,item_to_update);
        }
    }

    private int OldValue2NewValue(int OldValue,int num_to_aggregate,Op op)
            throws UnsupportedOperationException{
        switch (op) {
            case MAX:
                //System.out.println("max been called");
                //System.out.println(OldValue);
                //System.out.println(num_to_aggregate);
                return Math.max(OldValue,num_to_aggregate);
            case MIN:
                //System.out.println("min been called");
                return Math.min(OldValue,num_to_aggregate);
            case SUM:
                //System.out.println("sum been called");;
                return OldValue+num_to_aggregate;
            case COUNT:
                //System.out.println("cnt been called");
                return OldValue++;
            default:
                throw new UnsupportedOperationException("AVG need to operate out the func");
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntegerAggregateIterator();
    }
    class IntegerAggregateIterator implements OpIterator{
        private ArrayList<Tuple> Tuples_rem;
        private Iterator<Tuple> it;
        public IntegerAggregateIterator() {
            Tuples_rem = new ArrayList<>();
            for (ConcurrentHashMap.Entry<Field, items> it : map_gbfield2result.entrySet()) {
                Tuple t = new Tuple(td_rem);

                if (gbfield == Aggregator.NO_GROUPING) {
                    t.setField(0, new IntField(it.getValue().val));
                } else {
                    t.setField(0, it.getKey());
                    t.setField(1, new IntField(it.getValue().val));
                }
                Tuples_rem.add(t);
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            it=Tuples_rem.iterator();
        }

        @Override
        public boolean hasNext() throws DbException,TransactionAbortedException{
            return it.hasNext();
        }

        @Override
        public Tuple next(){
            return it.next();
        }

        @Override
        public void rewind() throws DbException,TransactionAbortedException{
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc(){
            return td_rem;
        }

        @Override
        public void close(){
            it=null;
        }
    }
}
