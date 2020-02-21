package simpledb;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    int min, max;
    int n, ntupls = 0;
    int[] buckets;
    double width;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.min = min;
        this.max = max + 1;
        this.n = buckets;
        this.buckets = new int[n];
        this.width = (1.0 + max - min) / buckets;
    }

    public IntHistogram setMin(int min) {
        this.min = min;
        return this;
    }

    public IntHistogram setMax(int max) {
        this.max = max;
        return this;
    }

    public void updateMin(int v) { min = min <= v ? min : v; }
    public void updateMax(int v) { max = max > v ? max : v+1; }
    public void updateWidth() { width = (1.0 + max - min) / n; }
    public void resetBuckets() { Arrays.fill(buckets, 0); }

    public int getMax() {
        return max;
    }

    public int getMin() {
        return min;
    }

    public double getWidth() {
        return width;
    }

    public int getIndex(int v)
    {
        if (v < min || v >= max)
            throw new IllegalArgumentException(String.format("%d out of range: [%d, %d)", v, min, max));
        return (int) ((v - min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	buckets[getIndex(v)]++;
        ntupls++;
    }

    private double estimateSelectivityGreater(int v)
    {
        if (v < min)
            return 1;
        if (v >= max-1)
            return 0;

        int idx = getIndex(v);
        double bs = buckets[idx] * ((idx + 1) * width - v) / width;
        for (int i = idx + 1; i < buckets.length; i++)
            bs += buckets[i];
        return bs / ntupls;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case EQUALS: case LIKE:
                /*
                if (v >= max || v < min)
                    return 0;
                int idx = getIndex(v);
                return buckets[idx] / width / ntupls;
                */
                return estimateSelectivityGreater(v - 1) - estimateSelectivityGreater(v);
            case LESS_THAN:
                return 1 - estimateSelectivityGreater(v-1);
            case LESS_THAN_OR_EQ:
                return 1 - estimateSelectivityGreater(v);
            case GREATER_THAN:
                return estimateSelectivityGreater(v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivityGreater(v-1);
            default: return -1.0;
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        StringBuilder builder = new StringBuilder("length: ").append(buckets.length).append(", [").append(min)
                .append(',').append(max).append(')');
        return builder.toString();
    }
}
