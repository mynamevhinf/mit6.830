package simpledb;
import sun.reflect.generics.tree.Tree;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    static class IntegerAggregateInfo {
        int cnt = 0, sum = 0;
        int min = Integer.MAX_VALUE, max = Integer.MIN_VALUE;
        static IntegerAggregateInfo newIntegerAggregateInfo() { return new IntegerAggregateInfo(); }
    }

    private static final long serialVersionUID = 1L;

    Op what;
    int afield;
    int gbfield;
    Type gbfieldtype;

    //Tuple cacheTuple;   /// used for get aggField and groupBy fields' names...
    Field dimKey; // used for not group-by aggregate
    OpIterator iterator = null;
    TreeMap<Field, IntegerAggregateInfo> gbOutput;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        gbOutput = new TreeMap<>((x, y) -> {
            if (x.compare(Predicate.Op.LESS_THAN, y)) return -1;
            if (x.compare(Predicate.Op.EQUALS, y)) return 0;
            else return 1;
        });
        if (gbfield == Aggregator.NO_GROUPING) {
            dimKey = new IntField(0);
            gbOutput.put(dimKey, IntegerAggregateInfo.newIntegerAggregateInfo());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        //cacheTuple = tup;
        int val = ((IntField) tup.getField(afield)).getValue();
        Field key = gbfield == NO_GROUPING ? dimKey : tup.getField(gbfield);
        IntegerAggregateInfo aggregateInfo = gbOutput.get(key);
        if (aggregateInfo == null) {
            aggregateInfo = IntegerAggregateInfo.newIntegerAggregateInfo();
            gbOutput.put(key, aggregateInfo);
        }
        aggregateInfo.cnt++;
        aggregateInfo.sum += val;
        aggregateInfo.max = Integer.max(aggregateInfo.max, val);
        aggregateInfo.min = Integer.min(aggregateInfo.min, val);
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
        if (iterator != null)
            return iterator;

        TupleDesc td;
        Type[] types; //String[] fieldNames;
        ArrayList<Tuple> tuples = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            types = new Type[]{Type.INT_TYPE};
            //fieldNames = new String[] { cacheTuple.getTupleDesc().getFieldName(afield) };
            td = new TupleDesc(types, null);
            Tuple tuple = new Tuple(td);
            IntField field = new IntField(evalOperator(gbOutput.get(dimKey), what));
            tuple.setField(0, field);
            tuples.add(tuple);
        } else {
            types = new Type[]{gbfieldtype, Type.INT_TYPE};
            //fieldNames = new String[] { cacheTuple.getTupleDesc().getFieldName(gbfield),
                    //cacheTuple.getTupleDesc().getFieldName(afield)};
            //td = new TupleDesc(types, fieldNames);
            td = new TupleDesc(types, null);
            for (Map.Entry<Field, IntegerAggregateInfo> entry : gbOutput.entrySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(evalOperator(entry.getValue(), what)));
                tuples.add(tuple);
            }
        }
        iterator = new TupleIterator(td, tuples);
        return iterator;
    }

    private int evalOperator(IntegerAggregateInfo info, Op what)
    {
        switch (what) {
            case AVG: return info.sum / info.cnt;
            case MAX: return info.max;
            case MIN: return info.min;
            case SUM: case SC_AVG: return info.sum;
            case COUNT: case SUM_COUNT: return info.cnt;
            default: System.exit(-1); return -1;
        }
    }

}
