package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private ConcurrentHashMap<Field,Integer> map_gbfield2result;
    private TupleDesc td_rem;
    private String[] nameAr;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        map_gbfield2result=new ConcurrentHashMap<>();
        nameAr=new String[2];
        if(what==Op.COUNT) this.what=what;
        else throw new IllegalArgumentException("don't support other operator except Op.COUNT");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field hash_gbfield=tup.getField(gbfield);
        StringField field_to_aggregate=(StringField) tup.getField(afield);
        String string_to_aggregate=field_to_aggregate.getValue();

        if(gbfield==Aggregator.NO_GROUPING) nameAr[0]=null;
        else nameAr[0]=tup.getTupleDesc().getFieldName(gbfield);
        nameAr[1]=tup.getTupleDesc().getFieldName(afield);

        if(tup.getField(afield).getType()!=Type.STRING_TYPE)
            throw new UnsupportedOperationException("the type of afield is not STRING_TYPE");

        if(!map_gbfield2result.containsKey(hash_gbfield)){
            if(what!=Op.COUNT){
                throw new IllegalArgumentException("don't support other operator except Op.COUNT");
            }
            Integer items_to_insert=1;
            map_gbfield2result.put(hash_gbfield,items_to_insert);
        }
        else{
            Integer item_to_update=map_gbfield2result.get(hash_gbfield);
            item_to_update+=1;
            map_gbfield2result.put(hash_gbfield,item_to_update);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggregateIterator();
    }

    class StringAggregateIterator implements OpIterator{
        private ArrayList<Tuple> Tuples_rem;
        private Iterator<Tuple> it;
        public StringAggregateIterator() {
            Tuples_rem = new ArrayList<>();
            for (ConcurrentHashMap.Entry<Field, Integer> it : map_gbfield2result.entrySet()) {
                Type[] typeAr;
                String[] fieldAr;
                if(gbfield==Aggregator.NO_GROUPING){
                    typeAr=new Type[]{Type.INT_TYPE};
                    fieldAr=new String[]{nameAr[1]};
                }
                else{
                    typeAr=new Type[]{gbfieldtype,Type.INT_TYPE};
                    fieldAr=new String[]{nameAr[0],nameAr[1]};
                }
                td_rem=new TupleDesc(typeAr,fieldAr);
                Tuple t = new Tuple(td_rem);

                if (gbfield == Aggregator.NO_GROUPING){
                    t.setField(0, new IntField(it.getValue()));
                    System.out.println(t.getField(0));
                }
                else{
                    t.setField(0, it.getKey());
                    t.setField(1, new IntField(it.getValue()));
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
