package simpledb;

import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class TryInfo {
    PageId pageId;
    Permissions permissions;

    TryInfo(PageId pid, Permissions perm) { pageId = pid; permissions = perm; }
}

class WaitGraphNode {
    boolean visited = false;

    TryInfo ti;
    PageId exPageId;
    HashSet<PageId> sharePageSet;

    WaitGraphNode(TryInfo ti)
    {
        this.ti = ti;
        sharePageSet = new HashSet<>();
    }
}

public class DeadLockManager extends Thread {
    long INIT_SLEEP_TIME = 1;
    long MAX_SLEEP_TIME = 4096;

    class PageStatus {
        TransactionId exTransaction;
        Set<TransactionId> shareLockSet;

        PageStatus(PageInfo pageInfo)
        {
            exTransaction = pageInfo.getExTransaction();
            shareLockSet = pageInfo.getShareLockSet().keySet();
        }
    }

    private BufferPool bufferPool;
    private HashMap<PageId, PageStatus> holdingStatus;
    private HashMap<TransactionId, WaitGraphNode> waitGraph;
    private ConcurrentHashMap<TransactionId, TryInfo> tryLockMaps;
    private long startTime;

    private DeadLockManager(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
        waitGraph = new HashMap<>();
        holdingStatus = new HashMap<>();
        tryLockMaps = new ConcurrentHashMap<>();
        startTime = System.currentTimeMillis();
    }

    void tryLock(TransactionId tid, PageId pid, Permissions permissions)
    {
        tryLockMaps.put(tid, new TryInfo(pid, permissions));
    }

    void getLock(TransactionId tid)
    {
        tryLockMaps.remove(tid);
    }

    public static DeadLockManager newDeadLockManager(BufferPool bufferPool) {
        DeadLockManager manager = new DeadLockManager(bufferPool);
        return manager;
    }

    WaitGraphNode getWaitGraphNode(TransactionId tid)
    {
        WaitGraphNode node = waitGraph.get(tid);
        if (node == null) {
            node = new WaitGraphNode(null);
            waitGraph.put(tid, node);
        }
        return node;
    }

    boolean checkIfDecdLock(TryInfo request, WaitGraphNode holder)
    {
        PageId requestPage = request.pageId;
        Permissions requestPerm = request.permissions;

        if (holder.visited == true) {
            PageId exPageId = holder.exPageId;
            /// i hold the page in exclude mode, u will waitting for it forever...
            if (exPageId != null && exPageId.equals(requestPage))
                return true;

            /// so u must be in holder shareLocks...
            if (requestPerm == Permissions.READ_WRITE)
                return true;
            /// no deadlock
            return false;
        }
        return false;
    }

    boolean checkForward(WaitGraphNode holder)
    {
        TryInfo myRequest = holder.ti;
        /// no edge anymore
        if (myRequest == null)
            return false;

        PageStatus status = holdingStatus.get(myRequest.pageId);
        /// may be not in the buffer when we collect information...
        if (status == null)
            return false;

        holder.visited = true;
        if (status.exTransaction != null) {
            WaitGraphNode node = waitGraph.get(status.exTransaction);
            if (findDecdLockSingle(myRequest, node))
                return true;
        } else {
            for (TransactionId pageId : status.shareLockSet) {
                WaitGraphNode node = waitGraph.get(pageId);
                if (findDecdLockSingle(myRequest, node))
                    return true;
            }
        }
        holder.visited = false;
        return false;
    }

    boolean findDecdLockSingle(TryInfo request, WaitGraphNode holder)
    {
        if (checkIfDecdLock(request, holder))
            return true;
        return checkForward(holder);
    }

    TransactionId findDecdLock()
    {
        for (TransactionId transactionId : tryLockMaps.keySet()) {
            WaitGraphNode waitGraphNode = waitGraph.get(transactionId);
            if (checkForward(waitGraphNode))
                return transactionId;
        }
        return null;
    }

    private void collectWaitGraph()
    {
        for (Map.Entry<TransactionId, TryInfo> entry : tryLockMaps.entrySet()) {
            TransactionId transactionId = entry.getKey();
            TryInfo ti = entry.getValue();
            WaitGraphNode node = new WaitGraphNode(ti);
            waitGraph.put(transactionId, node);
        }

        WaitGraphNode node;
        ConcurrentHashMap<PageId, PageInfo> pagesMap = bufferPool.getPagesMap();
        for (PageInfo pageInfo : pagesMap.values()) {
            //if (pageInfo.pid == null)
            //    continue;

            if (pageInfo.exTransaction != null) {
                node = getWaitGraphNode(pageInfo.exTransaction);
                node.exPageId = pageInfo.pid;
            }

            ConcurrentHashMap<TransactionId, Integer> shareLocks = pageInfo.getShareLockSet();
            for (TransactionId tid : shareLocks.keySet()) {
                node = getWaitGraphNode(tid);
                node.sharePageSet.add(pageInfo.pid);
            }

            holdingStatus.put(pageInfo.pid, new PageStatus(pageInfo));
        }
    }

    @Override
    public void run() {
        try {
            Thread.sleep(MAX_SLEEP_TIME >> 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long sleepTime = INIT_SLEEP_TIME;
        while (bufferPool.hasActiveTransactions()) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                e.printStackTrace();
                //System.exit(-1);
            }

            //while(!bufferPool.tryLockBufferPool());
            long start = System.currentTimeMillis();
            bufferPool.LockBufferPool();
            collectWaitGraph();
            bufferPool.unLockBufferPool();
            System.out.println("collect cost: " + (System.currentTimeMillis() - start));

            start = System.currentTimeMillis();
            TransactionId transactionId = findDecdLock();
            System.out.println("findDecdLock cost: " + (System.currentTimeMillis() - start));
            if (transactionId != null) {
                sleepTime = Long.max(sleepTime >> 1, INIT_SLEEP_TIME);
                try {
                    bufferPool.transactionComplete(transactionId, false);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(-1);
                }
            } else
                sleepTime = Long.min(sleepTime << 1, MAX_SLEEP_TIME);

            waitGraph.clear();
            tryLockMaps.clear();
            holdingStatus.clear();
        }
    }
}
