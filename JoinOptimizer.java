package simpledb;

import sun.rmi.runtime.Log;

import java.util.*;

import javax.swing.*;
import javax.swing.tree.*;

/**
 * The JoinOptimizer class is responsible for ordering a series of joins
 * optimally, and for selecting the best instantiation of a join for a given
 * logical plan.
 */
public class JoinOptimizer {
    private static double RANGE_CARDINALITY_HEURISTIC_RATE = .3629;

    LogicalPlan p;
    Vector<LogicalJoinNode> joins;

    /**
     * Constructor
     * 
     * @param p
     *            the logical plan being optimized
     * @param joins
     *            the list of joins being performed
     */
    public JoinOptimizer(LogicalPlan p, Vector<LogicalJoinNode> joins) {
        this.p = p;
        this.joins = joins;
    }

    /**
     * Return best iterator for computing a given logical join, given the
     * specified statistics, and the provided left and right subplans. Note that
     * there is insufficient information to determine which plan should be the
     * inner/outer here -- because OpIterator's don't provide any cardinality
     * estimates, and stats only has information about the base tables. For this
     * reason, the plan1
     * 
     * @param lj
     *            The join being considered
     * @param plan1
     *            The left join node's child
     * @param plan2
     *            The right join node's child
     */
    public static OpIterator instantiateJoin(LogicalJoinNode lj,
                                             OpIterator plan1, OpIterator plan2) throws ParsingException {

        int t1id = 0, t2id = 0;
        OpIterator j;

        try {
            t1id = plan1.getTupleDesc().fieldNameToIndex(lj.f1QuantifiedName);
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown field " + lj.f1QuantifiedName);
        }

        if (lj instanceof LogicalSubplanJoinNode) {
            t2id = 0;
        } else {
            try {
                t2id = plan2.getTupleDesc().fieldNameToIndex(
                        lj.f2QuantifiedName);
            } catch (NoSuchElementException e) {
                throw new ParsingException("Unknown field "
                        + lj.f2QuantifiedName);
            }
        }

        JoinPredicate p = new JoinPredicate(t1id, lj.p, t2id);

        j = new Join(p,plan1,plan2);

        return j;

    }

    /**
     * Estimate the cost of a join.
     * 
     * The cost of the join should be calculated based on the join algorithm (or
     * algorithms) that you implemented for Lab 2. It should be a function of
     * the amount of data that must be read over the course of the query, as
     * well as the number of CPU opertions performed by your join. Assume that
     * the cost of a single predicate application is roughly 1.
     * 
     * 
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Estimated cardinality of the left-hand side of the query
     * @param card2
     *            Estimated cardinality of the right-hand side of the query
     * @param cost1
     *            Estimated cost of one full scan of the table on the left-hand
     *            side of the query
     * @param cost2
     *            Estimated cost of one full scan of the table on the right-hand
     *            side of the query
     * @return An estimate of the cost of this query, in terms of cost1 and
     *         cost2
     */
    public double estimateJoinCost(LogicalJoinNode j, int card1, int card2,
            double cost1, double cost2) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1 + cost1 + cost2;
        } else {
            // Insert your code here.
            // HINT: You may need to use the variable "j" if you implemented
            // a join algorithm that's more complicated than a basic
            // nested-loops join.
            return cost1 + cost2 * card1 + cost1 + cost2;
        }
    }

    /**
     * Estimate the cardinality of a join. The cardinality of a join is the
     * number of tuples produced by the join.
     * 
     * @param j
     *            A LogicalJoinNode representing the join operation being
     *            performed.
     * @param card1
     *            Cardinality of the left-hand table in the join
     * @param card2
     *            Cardinality of the right-hand table in the join
     * @param t1pkey
     *            Is the left-hand table a primary-key table?
     * @param t2pkey
     *            Is the right-hand table a primary-key table?
     * @param stats
     *            The table stats, referenced by table names, not alias
     * @return The cardinality of the join
     */
    public int estimateJoinCardinality(LogicalJoinNode j, int card1, int card2,
            boolean t1pkey, boolean t2pkey, Map<String, TableStats> stats) {
        if (j instanceof LogicalSubplanJoinNode) {
            // A LogicalSubplanJoinNode represents a subquery.
            // You do not need to implement proper support for these for Lab 3.
            return card1;
        } else {
            return estimateTableJoinCardinality(j.p, j.t1Alias, j.t2Alias,
                    j.f1PureName, j.f2PureName, card1, card2, t1pkey, t2pkey,
                    stats, p.getTableAliasToIdMapping());
        }
    }

    /**
     * Estimate the join cardinality of two tables.
     * */
    public static int estimateTableJoinCardinality(Predicate.Op joinOp,
            String table1Alias, String table2Alias, String field1PureName,
            String field2PureName, int card1, int card2, boolean t1pkey,
            boolean t2pkey, Map<String, TableStats> stats,
            Map<String, Integer> tableAliasToId) {

        int card = 1;

        if (t1pkey && t2pkey) return Integer.min(card1, card2);
        else if (t1pkey) return card2;
        else if (t2pkey) return card1;

        switch (joinOp) {
            case NOT_EQUALS: case EQUALS: case LIKE:
                 card = Integer.max(card1, card2); break;
            case GREATER_THAN_OR_EQ: case LESS_THAN_OR_EQ:
            case GREATER_THAN: case LESS_THAN:
                Predicate.Op ngOp = swapOperator(joinOp);
                double rate = estimateTableJoinCardinalityAux(table1Alias, table2Alias, tableAliasToId,
                                stats, field1PureName, field2PureName, ngOp);
                card = (int) (card1 * card2 * rate);
            default:
        }
        return card;
    }

    private static Predicate.Op swapOperator(Predicate.Op joinOp)
    {
        switch (joinOp) {
            case GREATER_THAN_OR_EQ: return Predicate.Op.LESS_THAN;
            case LESS_THAN_OR_EQ: return  Predicate.Op.GREATER_THAN;
            case GREATER_THAN: return Predicate.Op.LESS_THAN_OR_EQ;
            case LESS_THAN: return Predicate.Op.GREATER_THAN_OR_EQ;
            default: return joinOp;
        }
    }

    private static double estimateTableJoinCardinalityAux(String table1Alias, String table2Alias,
                                                          Map<String, Integer> tableAliasToId, Map<String,
                                                          TableStats> stats, String field1PureName,
                                                          String field2PureName, Predicate.Op ngOp)
    {
        Catalog catalog = Database.getCatalog();
        int tableId1 = tableAliasToId.get(table1Alias);
        int tableId2 = tableAliasToId.get(table2Alias);
        HeapFile f1 = (HeapFile) catalog.getDatabaseFile(tableId1);
        HeapFile f2 = (HeapFile) catalog.getDatabaseFile(tableId1);
        TableStats ts1 = stats.get(catalog.getTableName(tableId1));
        TableStats ts2 = stats.get(catalog.getTableName(tableId2));
        int field1 = f1.getTupleDesc().getFieldId(field1PureName);
        int field2 = f2.getTupleDesc().getFieldId(field2PureName);

        IntHistogram ihist1, ihist2;
        if (f1.getTupleDesc().getFieldType(field1) == Type.INT_TYPE) {
            ihist1 = (IntHistogram) ts1.getHistogram(field1);
            ihist2 = (IntHistogram) ts2.getHistogram(field2);
        } else {
            ihist1 = ((StringHistogram) ts1.getHistogram(field1)).getHist();
            ihist2 = ((StringHistogram) ts2.getHistogram(field2)).getHist();
        }

        double rate = 0.0;
        int realMin = ihist1.getMin();
        int realMax = ihist1.getMax();
        if (ngOp == Predicate.Op.GREATER_THAN || ngOp == Predicate.Op.GREATER_THAN_OR_EQ) {
            if (ihist1.getMax() <= ihist2.getMin()) return 1.0;
            if (ihist1.getMin() >= ihist2.getMax()) return 0.0;

            if (ihist1.getMin() < ihist2.getMin()) {
                realMin = ihist2.getMin();
                //realMin = (int) (ihist1.getIndex(ihist2.getMin()) * ihist1.getWidth());
                rate += ihist1.estimateSelectivity(Predicate.Op.LESS_THAN, realMin);
            }

            if (ihist1.getMax() > ihist2.getMax())
                realMax = ihist2.getMax();//(int) (ihist1.getIndex(ihist2.getMin()) * ihist1.getWidth());
        } else {
            if (ihist1.getMax() <= ihist2.getMin()) return 0.0;
            if (ihist1.getMin() >= ihist2.getMax()) return 1.0;

            if (ihist1.getMin() < ihist2.getMin())
                realMin = ihist2.getMin();

            if (ihist1.getMax() > ihist2.getMax()) {
                //realMax = (int) (ihist1.getIndex(ihist2.getMax()) * ihist1.getWidth());
                realMax = ihist2.getMax();
                rate += ihist1.estimateSelectivity(Predicate.Op.GREATER_THAN, realMax);
            }
        }

        double width = ihist1.getWidth();
        for (int val = realMin; val < realMax; val += width)
            rate += ihist2.estimateSelectivity(ngOp, (int) (val+width/2)) * ihist1.getBucket(val);
        return rate != 0.0 && rate <= 1.0 ? rate : 0.3721;
    }

    /**
     * Helper method to enumerate all of the subsets of a given size of a
     * specified vector.
     * 
     * @param v
     *            The vector whose subsets are desired
     * @param size
     *            The size of the subsets of interest
     * @return a set of all subsets of the specified size
     */
    @SuppressWarnings("unchecked")
    public <T> Set<Set<T>> enumerateSubsets(Vector<T> v, int size) {
        Set<Set<T>> els = new HashSet<Set<T>>();
        els.add(new HashSet<T>());
        // Iterator<Set> it;
        // long start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            Set<Set<T>> newels = new HashSet<Set<T>>();
            for (Set<T> s : els) {
                for (T t : v) {
                    Set<T> news = (Set<T>) (((HashSet<T>) s).clone());
                    if (news.add(t))
                        newels.add(news);
                }
            }
            els = newels;
        }

        return els;

    }

    /**
     * Compute a logical, reasonably efficient join on the specified tables. See
     * PS4 for hints on how this should be implemented.
     * 
     * @param stats
     *            Statistics for each table involved in the join, referenced by
     *            base table names, not alias
     * @param filterSelectivities
     *            Selectivities of the filter predicates on each table in the
     *            join, referenced by table alias (if no alias, the base table
     *            name)
     * @param explain
     *            Indicates whether your code should explain its query plan or
     *            simply execute it
     * @return A Vector<LogicalJoinNode> that stores joins in the left-deep
     *         order in which they should be executed.
     * @throws ParsingException
     *             when stats or filter selectivities is missing a table in the
     *             join, or or when another internal error occurs
     */
    public Vector<LogicalJoinNode> orderJoins(
            HashMap<String, TableStats> stats,
            HashMap<String, Double> filterSelectivities, boolean explain)
            throws ParsingException {

        int size = joins.size();
        Set<LogicalJoinNode> resKey = null;
        PlanCache pc = null, oldPc = new PlanCache();
        AuxIterator<LogicalJoinNode> iterator0 = AuxIterator.newAuxIterator(joins);

        for (int i = 1; i <= size; i++) {
            pc = new PlanCache();
            iterator0.rewind(i, size);
            while (iterator0.hasNext()) {
                Set<LogicalJoinNode> s1 = (Set<LogicalJoinNode>) ((HashSet) iterator0.next()).clone();
                double bestCostSoFar = Double.MAX_VALUE;
                int bestCard = 0; Vector<LogicalJoinNode> bestPlan = null;

                Object[] nodes = s1.toArray();
                for (Object o : nodes) {
                    LogicalJoinNode removed = (LogicalJoinNode) o;
                    s1.remove(o);
                    CostCard costCard = computeCostAndCardOfSubplan(stats, filterSelectivities,
                            removed, s1, bestCostSoFar, oldPc);
                    s1.add((LogicalJoinNode) o);

                    if (costCard != null && bestCostSoFar > costCard.cost) {
                        bestCard = costCard.card;
                        bestPlan = costCard.plan;
                        bestCostSoFar = costCard.cost;
                    }
                }

                pc.addPlan(s1, bestCostSoFar, bestCard, bestPlan);
                resKey = s1;
            }
            oldPc = pc;
        }

        joins = pc.getOrder(resKey);
        if (explain)
            printJoins(joins, pc, stats, filterSelectivities);
        return joins;
    }

    // ===================== Private Methods =================================

    /**
     * This is a helper method that computes the cost and cardinality of joining
     * joinToRemove to joinSet (joinSet should contain joinToRemove), given that
     * all of the subsets of size joinSet.size() - 1 have already been computed
     * and stored in PlanCache pc.
     * 
     * @param stats
     *            table stats for all of the tables, referenced by table names
     *            rather than alias (see {@link #orderJoins})
     * @param filterSelectivities
     *            the selectivities of the filters over each of the tables
     *            (where tables are indentified by their alias or name if no
     *            alias is given)
     * @param joinToRemove
     *            the join to remove from joinSet
     * @param joinSet
     *            the set of joins being considered
     * @param bestCostSoFar
     *            the best way to join joinSet so far (minimum of previous
     *            invocations of computeCostAndCardOfSubplan for this joinSet,
     *            from returned CostCard)
     * @param pc
     *            the PlanCache for this join; should have subplans for all
     *            plans of size joinSet.size()-1
     * @return A {@link CostCard} objects desribing the cost, cardinality,
     *         optimal subplan
     * @throws ParsingException
     *             when stats, filterSelectivities, or pc object is missing
     *             tables involved in join
     */
    @SuppressWarnings("unchecked")
    private CostCard computeCostAndCardOfSubplan(
            HashMap<String, TableStats> stats,
            HashMap<String, Double> filterSelectivities,
            LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet,
            double bestCostSoFar, PlanCache pc) throws ParsingException {

        LogicalJoinNode j = joinToRemove;

        Vector<LogicalJoinNode> prevBest;

        if (this.p.getTableId(j.t1Alias) == null)
            throw new ParsingException("Unknown table " + j.t1Alias);
        if (this.p.getTableId(j.t2Alias) == null)
            throw new ParsingException("Unknown table " + j.t2Alias);

        String table1Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t1Alias));
        String table2Name = Database.getCatalog().getTableName(
                this.p.getTableId(j.t2Alias));
        String table1Alias = j.t1Alias;
        String table2Alias = j.t2Alias;

        Set<LogicalJoinNode> news = joinSet;
        //Set<LogicalJoinNode> news = (Set<LogicalJoinNode>) ((HashSet<LogicalJoinNode>) joinSet)
        //        .clone();
        //news.remove(j);

        double t1cost, t2cost;
        int t1card, t2card;
        boolean leftPkey, rightPkey;

        if (news.isEmpty()) { // base case -- both are base relations
            prevBest = new Vector<LogicalJoinNode>();
            t1cost = stats.get(table1Name).estimateScanCost();
            t1card = stats.get(table1Name).estimateTableCardinality(
                    filterSelectivities.get(j.t1Alias));
            leftPkey = isPkey(j.t1Alias, j.f1PureName);

            t2cost = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateScanCost();
            t2card = table2Alias == null ? 0 : stats.get(table2Name)
                    .estimateTableCardinality(
                            filterSelectivities.get(j.t2Alias));
            rightPkey = table2Alias == null ? false : isPkey(table2Alias,
                    j.f2PureName);
        } else {
            // news is not empty -- figure best way to join j to news
            prevBest = pc.getOrder(news);

            // possible that we have not cached an answer, if subset
            // includes a cross product
            if (prevBest == null) {
                return null;
            }

            double prevBestCost = pc.getCost(news);
            int bestCard = pc.getCard(news);

            // estimate cost of right subtree
            if (doesJoin(prevBest, table1Alias)) { // j.t1 is in prevBest
                t1cost = prevBestCost; // left side just has cost of whatever
                                       // left
                // subtree is
                t1card = bestCard;
                leftPkey = hasPkey(prevBest);

                t2cost = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateScanCost();
                t2card = j.t2Alias == null ? 0 : stats.get(table2Name)
                        .estimateTableCardinality(
                                filterSelectivities.get(j.t2Alias));
                rightPkey = j.t2Alias == null ? false : isPkey(j.t2Alias,
                        j.f2PureName);
            } else if (doesJoin(prevBest, j.t2Alias)) { // j.t2 is in prevbest
                                                        // (both
                // shouldn't be)
                t2cost = prevBestCost; // left side just has cost of whatever
                                       // left
                // subtree is
                t2card = bestCard;
                rightPkey = hasPkey(prevBest);
                t1cost = stats.get(table1Name).estimateScanCost();
                t1card = stats.get(table1Name).estimateTableCardinality(
                        filterSelectivities.get(j.t1Alias));
                leftPkey = isPkey(j.t1Alias, j.f1PureName);

            } else {
                // don't consider this plan if one of j.t1 or j.t2
                // isn't a table joined in prevBest (cross product)
                return null;
            }
        }

        // case where prevbest is left
        double cost1 = estimateJoinCost(j, t1card, t2card, t1cost, t2cost);

        LogicalJoinNode j2 = j.swapInnerOuter();
        double cost2 = estimateJoinCost(j2, t2card, t1card, t2cost, t1cost);
        if (cost2 < cost1) {
            boolean tmp;
            j = j2;
            cost1 = cost2;
            tmp = rightPkey;
            rightPkey = leftPkey;
            leftPkey = tmp;
        }
        if (cost1 >= bestCostSoFar)
            return null;

        CostCard cc = new CostCard();

        cc.card = estimateJoinCardinality(j, t1card, t2card, leftPkey,
                rightPkey, stats);
        cc.cost = cost1;
        cc.plan = (Vector<LogicalJoinNode>) prevBest.clone();
        cc.plan.addElement(j); // prevbest is left -- add new join to end
        return cc;
    }

    /**
     * Return true if the specified table is in the list of joins, false
     * otherwise
     */
    private boolean doesJoin(Vector<LogicalJoinNode> joinlist, String table) {
        for (LogicalJoinNode j : joinlist) {
            if (j.t1Alias.equals(table)
                    || (j.t2Alias != null && j.t2Alias.equals(table)))
                return true;
        }
        return false;
    }

    /**
     * Return true if field is a primary key of the specified table, false
     * otherwise
     * 
     * @param tableAlias
     *            The alias of the table in the query
     * @param field
     *            The pure name of the field
     */
    private boolean isPkey(String tableAlias, String field) {
        int tid1 = p.getTableId(tableAlias);
        String pkey1 = Database.getCatalog().getPrimaryKey(tid1);

        return pkey1.equals(field);
    }

    /**
     * Return true if a primary key field is joined by one of the joins in
     * joinlist
     */
    private boolean hasPkey(Vector<LogicalJoinNode> joinlist) {
        for (LogicalJoinNode j : joinlist) {
            if (isPkey(j.t1Alias, j.f1PureName)
                    || (j.t2Alias != null && isPkey(j.t2Alias, j.f2PureName)))
                return true;
        }
        return false;

    }

    /**
     * Helper function to display a Swing window with a tree representation of
     * the specified list of joins. See {@link #orderJoins}, which may want to
     * call this when the analyze flag is true.
     * 
     * @param js
     *            the join plan to visualize
     * @param pc
     *            the PlanCache accumulated whild building the optimal plan
     * @param stats
     *            table statistics for base tables
     * @param selectivities
     *            the selectivities of the filters over each of the tables
     *            (where tables are indentified by their alias or name if no
     *            alias is given)
     */
    private void printJoins(Vector<LogicalJoinNode> js, PlanCache pc,
            HashMap<String, TableStats> stats,
            HashMap<String, Double> selectivities) {

        JFrame f = new JFrame("Join Plan for " + p.getQuery());

        // Set the default close operation for the window,
        // or else the program won't exit when clicking close button
        f.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);

        f.setVisible(true);

        f.setSize(300, 500);

        HashMap<String, DefaultMutableTreeNode> m = new HashMap<String, DefaultMutableTreeNode>();

        // int numTabs = 0;

        // int k;
        DefaultMutableTreeNode root = null, treetop = null;
        HashSet<LogicalJoinNode> pathSoFar = new HashSet<LogicalJoinNode>();
        boolean neither;

        System.out.println(js);
        for (LogicalJoinNode j : js) {
            pathSoFar.add(j);
            System.out.println("PATH SO FAR = " + pathSoFar);

            String table1Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t1Alias));
            String table2Name = Database.getCatalog().getTableName(
                    this.p.getTableId(j.t2Alias));

            // Double c = pc.getCost(pathSoFar);
            neither = true;

            root = new DefaultMutableTreeNode("Join " + j + " (Cost ="
                    + pc.getCost(pathSoFar) + ", card = "
                    + pc.getCard(pathSoFar) + ")");
            DefaultMutableTreeNode n = m.get(j.t1Alias);
            if (n == null) { // never seen this table before
                n = new DefaultMutableTreeNode(j.t1Alias
                        + " (Cost = "
                        + stats.get(table1Name).estimateScanCost()
                        + ", card = "
                        + stats.get(table1Name).estimateTableCardinality(
                                selectivities.get(j.t1Alias)) + ")");
                root.add(n);
            } else {
                // make left child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t1Alias, root);

            n = m.get(j.t2Alias);
            if (n == null) { // never seen this table before

                n = new DefaultMutableTreeNode(
                        j.t2Alias == null ? "Subplan"
                                : (j.t2Alias
                                        + " (Cost = "
                                        + stats.get(table2Name)
                                                .estimateScanCost()
                                        + ", card = "
                                        + stats.get(table2Name)
                                                .estimateTableCardinality(
                                                        selectivities
                                                                .get(j.t2Alias)) + ")"));
                root.add(n);
            } else {
                // make right child root n
                root.add(n);
                neither = false;
            }
            m.put(j.t2Alias, root);

            // unless this table doesn't join with other tables,
            // all tables are accessed from root
            if (!neither) {
                for (String key : m.keySet()) {
                    m.put(key, root);
                }
            }

            treetop = root;
        }

        JTree tree = new JTree(treetop);
        JScrollPane treeView = new JScrollPane(tree);

        tree.setShowsRootHandles(true);

        // Set the icon for leaf nodes.
        ImageIcon leafIcon = new ImageIcon("join.jpg");
        DefaultTreeCellRenderer renderer = new DefaultTreeCellRenderer();
        renderer.setOpenIcon(leafIcon);
        renderer.setClosedIcon(leafIcon);

        tree.setCellRenderer(renderer);

        f.setSize(300, 500);

        f.add(treeView);
        for (int i = 0; i < tree.getRowCount(); i++) {
            tree.expandRow(i);
        }

        if (js.size() == 0) {
            f.add(new JLabel("No joins in plan."));
        }

        f.pack();

    }

}

class AuxIterator<T> {
    int[] indexs;
    int lastMoved;
    BitSet bitSet;
    boolean swap = false;
    Vector<T> joins;
    Set<T> next, reused;

    static <T> AuxIterator<T> newAuxIterator(Vector<T> joins) {
        AuxIterator<T> iterator = new AuxIterator<T>();
        iterator.joins = joins;
        iterator.reused = new HashSet<>();
        return iterator;
    }

    public void rewind(int sl, int l)
    {
        int rsl = sl;
        if ((sl << 1) > l) {
            rsl = l - sl;
            swap = true;
        }

        int[] indexs = new int[rsl];
        for (int i = 0; i <= rsl-1; i++)
            indexs[i] = i;
        BitSet bitSet = new BitSet(l);

        this.next = null;
        this.indexs = indexs;
        this.bitSet = bitSet;
        this.lastMoved = rsl - 1;

        if (sl == l) {
            Set<T> onlySet = new HashSet<>();
            Iterator<T> it = joins.iterator();
            while (it.hasNext())
                onlySet.add(it.next());
            this.next = onlySet;
            return;
        }

        if (rsl > 0)
            bitSet.flip(0, rsl-1);
    }

    boolean hasNext()
    {
        if (indexs.length == 0 || lastMoved < 0) {
            if (next == null)
                return false;
        }
        if (next == null)
            fetchNext();
        return next != null ? true : false;
    }

    private void fetchNext()
    {
        int l = joins.size();

        int move = lastMoved;
        while (true) {
            int idx = indexs[move];
            int avail = bitSet.nextClearBit(idx);
            avail = avail <= l ? avail : l;
            bitSet.clear(idx);

            if (avail >= l) {
                if (move == 0) {
                    lastMoved = -1;
                    return;
                }

                int oldmoved = move;
                for (move-=1; move >= 0; move--) {
                    bitSet.clear(indexs[move]);
                    if (indexs[move+1] - indexs[move] > 1)
                        break;
                }

                if (move < 0) {
                    lastMoved = -1;
                    return;
                }

                bitSet.clear(indexs[move]);;
                int baseIdx = ++indexs[move];
                bitSet.set(baseIdx);
                for (int i = move+1; i <= oldmoved; i++)
                    indexs[i] = indexs[i - 1] + 1;
                bitSet.set(indexs[move+1], indexs[oldmoved]);
                move = oldmoved;
                continue;
            }
            bitSet.set(avail);
            indexs[move] = avail;
            lastMoved = move;
            break;
        }

        Set<T> reused = this.reused;
        reused.clear();
        if (swap) {
            int idx = bitSet.nextClearBit(0);
            while (idx < l) {
                reused.add(joins.get(idx++));
                idx = bitSet.nextClearBit(idx);
            }
        } else {
            for (int idx : indexs) {
                reused.add(joins.get(idx));
            }
        }
        next = reused;
    }

    Set<T> next()
    {
        Set<T> rs = next;
        next = null;
        return rs;
    }
}