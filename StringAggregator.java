package simpledb;

import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

import static simpledb.Aggregator.Op.COUNT;
import static simpledb.Aggregator.Op.SUM_COUNT;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    static class StringAggregatorInfo {
        int cnt;
        String res;
        public StringAggregatorInfo() { cnt = 0; res = null; }
        public StringAggregatorInfo(String init)
        {
            cnt = 1;
            res = init;
        }
    }

    private static final long serialVersionUID = 1L;

    Op what;
    int afield;
    int gbfield;
    Type gbfieldtype;
    Field dimKey;
    //Tuple cacheTuple;
    OpIterator iterator;
    Map<Field, StringAggregatorInfo> gbOutput;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;

        gbOutput = new TreeMap<>((x, y) -> {
            if (x.compare(Predicate.Op.LESS_THAN, y)) return -1;
            if (x.compare(Predicate.Op.EQUALS, y)) return 0;
            else return 1;
        });
        if (gbfield == NO_GROUPING) {
            dimKey = new StringField("dim", 3);
            gbOutput.put(dimKey, new StringAggregatorInfo());
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        //cacheTuple = tup;
        Field key = gbfield == NO_GROUPING ? dimKey : tup.getField(gbfield);
        StringAggregatorInfo info = gbOutput.get(key);
        String val = tup.getField(afield).toString();
        if (info == null) {
            info = new StringAggregatorInfo(val);
            gbOutput.put(key, info);
            return;
        }
        evalOperator(info, val, what);
    }

    private void evalOperator(StringAggregatorInfo info, String val, Op what) {
        if (info.cnt == 0) {
            info.cnt++;
            info.res = val;
            return;
        }

        switch (what) {
            case SUM_COUNT: case COUNT: info.cnt++; break;
            case SC_AVG: case AVG: break;
            case SUM: break;
            case MIN: if (info.res.compareTo(val) > 0) info.res = val; break;
            case MAX: if (info.res.compareTo(val) < 0) info.res = val; break;
            default:
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
        if (iterator != null)
            return iterator;

        TupleDesc td;
        Type[] types; //String[] fieldNames;
        ArrayList<Tuple> tuples = new ArrayList<>();
        Type aggType = what == SUM_COUNT || what == COUNT ? Type.INT_TYPE : Type.STRING_TYPE;
        if (gbfield == NO_GROUPING) {
            //fieldNames = new String[] { cacheTuple.getTupleDesc().getFieldName(gbfield) };
            types = new Type[] { aggType };
            td = new TupleDesc(types, null);
            Tuple tuple = new Tuple(td);
            StringAggregatorInfo info = gbOutput.get(dimKey);
            Field field = (aggType == Type.INT_TYPE) ?
                    new IntField(info.cnt) : new StringField(info.res, info.res.length());
            tuple.setField(0, field);
            tuples.add(tuple);
        } else {
            //fieldNames = new String[] { cacheTuple.getTupleDesc().getFieldName(gbfield),
            //        cacheTuple.getTupleDesc().getFieldName(afield)};
            types = new Type[] { gbfieldtype, aggType };
            td = new TupleDesc(types, null);
            for (Map.Entry<Field, StringAggregatorInfo> entry : gbOutput.entrySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                StringAggregatorInfo info = entry.getValue();
                Field field = (aggType == Type.INT_TYPE) ?
                        new IntField(info.cnt) : new StringField(info.res, info.res.length());
                tuple.setField(1, field);
                tuples.add(tuple);
            }
        }
        iterator = new TupleIterator(td, tuples);
        return iterator;
    }

}
