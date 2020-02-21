package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    int tableId;
    int ioCostPerPage;
    int version, ntuples, npages;
    Object[] histograms;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableId = tableid;
        this.ioCostPerPage = ioCostPerPage;

        Type[] types = Database.getCatalog().getTupleDesc(tableid).getTypes();
        histograms = new Object[types.length];

        int i = 0;
        for (Type type : types) {
            if (type == Type.INT_TYPE)
                histograms[i++] = new IntHistogram(NUM_HIST_BINS, Integer.MAX_VALUE, Integer.MIN_VALUE);
            else
                histograms[i++] = new StringHistogram(NUM_HIST_BINS);
        }
        updateStats();
    }

    private void updateStats()
    {
        int nTuples = 0;
        HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        DbFileIterator iterator = f.iterator(new TransactionId());

        Arrays.stream(histograms).filter(o -> o instanceof IntHistogram)
                .forEach(o -> ((IntHistogram)o).setMax(Integer.MIN_VALUE).setMin(Integer.MAX_VALUE).resetBuckets());

        try {
            int i;
            iterator.open();
            while (iterator.hasNext()) {
                i = 0;
                Tuple t = iterator.next();
                for (Field field : t.getFields()) {
                    if (field.getType() == Type.INT_TYPE) {
                        int v = ((IntField) field).getValue();
                        IntHistogram intHistogram = (IntHistogram) histograms[i];
                        intHistogram.updateMax(v);
                        intHistogram.updateMin(v);
                    }
                    i++;
                }
            }

            Arrays.stream(histograms).forEach(o ->
            { if (o instanceof IntHistogram) ((IntHistogram) o).updateWidth();
                else ((StringHistogram) o).getHist().resetBuckets(); });

            iterator.rewind();
            while (iterator.hasNext()) {
                i = 0;
                Tuple t = iterator.next();
                for (Field field : t.getFields()) {
                    if (field.getType() == Type.INT_TYPE) {
                        int v = ((IntField) field).getValue();
                        IntHistogram intHistogram = (IntHistogram) histograms[i++];
                        intHistogram.addValue(v);
                    } else {
                        String v = ((StringField) field).getValue();
                        StringHistogram stringHistogram = (StringHistogram) histograms[i++];
                        stringHistogram.addValue(v);
                    }
                }
                nTuples++;
            }
            iterator.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        this.ntuples = nTuples;
        this.npages = f.numPages();
        this.version = f.getVersion();
    }

    private boolean needSync()
    {
        HeapFile f = (HeapFile) Database.getCatalog().getDatabaseFile(tableId);
        return f.getVersion() > version;
    }

    public Object getHistogram(int i) {
        return histograms[i];
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        if (needSync())
            updateStats();
        return npages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        if (needSync())
            updateStats();
        return (int) (ntuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if (needSync())
            updateStats();
        if (constant.getType() == Type.INT_TYPE) {
            int v = ((IntField) constant).getValue();
            IntHistogram intHistogram = (IntHistogram) histograms[field];
            return intHistogram.estimateSelectivity(op, v);
        } else {
            String v = ((StringField) constant).getValue();
            StringHistogram stringHistogram = (StringHistogram) histograms[field];
            return stringHistogram.estimateSelectivity(op, v);
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        return ntuples;
    }

}
