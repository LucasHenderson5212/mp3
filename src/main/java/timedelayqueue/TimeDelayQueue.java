package timedelayqueue;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

// TODO: write a description for this class
// TODO: complete all methods, irrespective of whether there is an explicit TODO or not
// TODO: write clear specs
// TODO: State the rep invariant and abstraction function
// TODO: what is the thread safety argument?
public class TimeDelayQueue {

    // a comparator to sort messages
    private static class PubSubMessageComparator implements Comparator<PubSubMessage> {
        public int compare(PubSubMessage msg1, PubSubMessage msg2) {
            return msg1.getTimestamp().compareTo(msg2.getTimestamp());
        }
    }

    private List<PubSubMessage> queue = new ArrayList<>();
    private long totalMsgAddedCount = 0;
    private final List<Timestamp> queueOperationHistory = new ArrayList<>();
    private final int delay;

    /**
     * Create a new TimeDelayQueue
     *
     * @param delay the delay, in milliseconds, that the queue can tolerate, >= 0
     */
    public TimeDelayQueue(int delay) {
        this.delay = delay;
    }

    // add a message to the TimeDelayQueue
    // if a message with the same id exists then
    // return false
    public boolean add(PubSubMessage msg) {

        synchronized (this) {
            if (!queue.contains(msg)) {
                Timestamp t = new Timestamp(System.currentTimeMillis());
                queueOperationHistory.add(t);
                queue.add(msg);
                totalMsgAddedCount++;
                queue = queue.stream().sorted(new PubSubMessageComparator())
                        .collect(Collectors.toList());
                return true;
            }
            return false;
        }
    }

    /**
     * Get the count of the total number of messages processed
     * by this TimeDelayQueue
     *
     * @return the total count of messages added to queue
     */
    public long getTotalMsgCount() {
        return totalMsgAddedCount;
    }

    // return the next message and PubSubMessage.NO_MSG
    // if there is ni suitable message
    public PubSubMessage getNext() {
        synchronized (this) {
            Timestamp t = new Timestamp(System.currentTimeMillis());
            queueOperationHistory.add(t);
            List<TransientPubSubMessage> tempQueue;
            tempQueue = queue.stream().filter(PubSubMessage::isTransient).map(x -> (TransientPubSubMessage) x)
                            .filter(x -> x.getLifetime() + x.getTimestamp().getTime() < t.getTime())
                            .collect(Collectors.toList());
            queue.removeAll(tempQueue);

            if (queue.size() == 0) {
                return PubSubMessage.NO_MSG;
            }
            else if (t.getTime() - queue.get(0).getTimestamp().getTime() > delay) {
                PubSubMessage temp = queue.get(0);
                queue.remove(0);
                return temp;
            } else {
                return PubSubMessage.NO_MSG;
            }
        }
    }

    // return the maximum number of operations
    // performed on this TimeDelayQueue over
    // any window of length timeWindow
    // the operations of interest are add and getNext
    public int getPeakLoad(int timeWindow) {
        synchronized (this) {
            int peakLoad = 0;

            for (int i = 0; i < queueOperationHistory.size(); i++) {
                long startingTime = queueOperationHistory.get(i).getTime();
                int next = i + 1;
                int tempPeakLoad = 1;
                while (i + next < queueOperationHistory.size() &&
                        queueOperationHistory.get(i + next).getTime() -
                                startingTime < timeWindow
                ) {
                    tempPeakLoad++;
                    next++;
                }
                if (tempPeakLoad > peakLoad) {
                    peakLoad = tempPeakLoad;
                }
            }
            return peakLoad;
        }
    }

}
