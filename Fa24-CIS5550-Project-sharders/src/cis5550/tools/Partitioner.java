package cis5550.tools;

import java.util.*;

public class Partitioner {

    public class Partition {
        public String kvsWorker;
        public String fromKey;
        public String toKeyExclusive;
        public String assignedFlameWorker;

        Partition(String kvsWorkerArg, String fromKeyArg, String toKeyExclusiveArg, String assignedFlameWorkerArg) {
            kvsWorker = kvsWorkerArg;
            fromKey = fromKeyArg;
            toKeyExclusive = toKeyExclusiveArg;
            assignedFlameWorker = assignedFlameWorkerArg;
        }

        Partition(String kvsWorkerArg, String fromKeyArg, String toKeyExclusiveArg) {
            kvsWorker = kvsWorkerArg;
            fromKey = fromKeyArg;
            toKeyExclusive = toKeyExclusiveArg;
            assignedFlameWorker = null;
        }

        public String toString() {
            return "[kvs:" + kvsWorker + ", keys: " + (fromKey == null ? "" : fromKey) + "-"
                    + (toKeyExclusive == null ? "" : toKeyExclusive) + ", flame: " + assignedFlameWorker + "]";
        }
    };

    boolean sameIP(String a, String b) {
        String aPcs[] = a.split(":");
        String bPcs[] = b.split(":");
        return aPcs[0].equals(bPcs[0]);
    }

    Vector<String> flameWorkers;
    Vector<Partition> partitions;
    boolean alreadyAssigned;
    int keyRangesPerWorker;

    public Partitioner() {
        partitions = new Vector<Partition>();
        flameWorkers = new Vector<String>();
        alreadyAssigned = false;
        keyRangesPerWorker = 1;
    }

    public void setKeyRangesPerWorker(int keyRangesPerWorkerArg) {
        keyRangesPerWorker = keyRangesPerWorkerArg;
    }

    public void addKVSWorker(String kvsWorker, String fromKeyOrNull, String toKeyOrNull) {
        partitions.add(new Partition(kvsWorker, fromKeyOrNull, toKeyOrNull));
    }

    public void addFlameWorker(String worker) {
        flameWorkers.add(worker);
    }

    private static List<String> generateSplits(String fromKey, String toKeyExclusive, int additionalSplitsNeededPerOriginalPartition) {
    	if (fromKey == null) fromKey = "aaaaa";
    	if (toKeyExclusive == null) toKeyExclusive = "zzzzz";
        if (fromKey.length() != toKeyExclusive.length()) {
            throw new IllegalArgumentException("Both fromKey and toKeyExclusive must be exactly 5 characters long.");
        }
        
        long start = stringToLong(fromKey);
        long end = stringToLong(toKeyExclusive);
        long step = (end - start) / (additionalSplitsNeededPerOriginalPartition + 1);

        List<String> splits = new ArrayList<>();
        if (step == 0) return splits;
        for (int i = 1; i <= additionalSplitsNeededPerOriginalPartition; i++) {
            long splitValue = start + (i * step);
            splits.add(longToString(splitValue));
        }

        return splits;
    }

    private static long stringToLong(String s) {
        long value = 0;
        for (int i = 0; i < s.length(); i++) {
            value = value * 26 + (s.charAt(i) - 'a');
        }
        return value;
    }

    private static String longToString(long value) {
        char[] chars = new char[5];
        for (int i = 4; i >= 0; i--) {
            chars[i] = (char) ('a' + (value % 26));
            value /= 26;
        }
        return new String(chars);
    }
    
    public Vector<Partition> assignPartitions() {
        if (alreadyAssigned || (flameWorkers.size() < 1) || partitions.size() < 1)
            return null;

        Random rand = new Random();

        /*
         * let's figure out how many partitions we need based on keyRangesPerWorker and
         * the number of flame workers
         */
        int requiredNumberOfPartitions = flameWorkers.size() * this.keyRangesPerWorker;

        // let's sort the partitions by key so we can identify a partition using binary
        // search
        partitions.sort((e1, e2) -> {
            if (e1.fromKey == null) {
                return -1;
            } else if (e2.fromKey == null) {
                return 1;
            } else {
                return e1.fromKey.compareTo(e2.fromKey);
            }
        });

        int additionalSplitsNeededPerOriginalPartition = (int) Math.ceil(requiredNumberOfPartitions / partitions.size());

        if (additionalSplitsNeededPerOriginalPartition > 0) {
            Vector<Partition> allPartitions = new Vector<>();

            for (int i = 0; i < partitions.size(); i++) {
                Partition p = partitions.get(i);
                String fromKey = p.fromKey;
                String toKeyExclusive = p.toKeyExclusive;
                List<String> newSplits = generateSplits(fromKey, toKeyExclusive, additionalSplitsNeededPerOriginalPartition);

                newSplits.sort((e1, e2) -> e1.compareTo(e2));
                if (newSplits.size() > 0) {
	                allPartitions.add(new Partition(p.kvsWorker, fromKey, newSplits.get(0)));
	                for (int j = 0; j < newSplits.size() - 1; j++) {
	                    allPartitions.add(new Partition(p.kvsWorker, newSplits.get(j),
	                            newSplits.get(j + 1)));
	                }
	                allPartitions.add(new Partition(p.kvsWorker, newSplits.get(newSplits.size() - 1),
	                        toKeyExclusive));
                } else {allPartitions.add(new Partition(p.kvsWorker, fromKey,
                        toKeyExclusive));}
            }
            partitions = allPartitions;
        }

        /*
         * Now we'll try to evenly assign partitions to workers, giving preference to
         * workers on the same host
         */

        int numAssigned[] = new int[flameWorkers.size()];
        for (int i = 0; i < numAssigned.length; i++)
            numAssigned[i] = 0;

        for (int i = 0; i < partitions.size(); i++) {
            int bestCandidate = 0;
            int bestWorkload = 9999;
            for (int j = 0; j < numAssigned.length; j++) {
                if ((numAssigned[j] < bestWorkload) || ((numAssigned[j] == bestWorkload)
                        && sameIP(flameWorkers.elementAt(j), partitions.elementAt(i).kvsWorker))) {
                    bestCandidate = j;
                    bestWorkload = numAssigned[j];
                }
            }

            numAssigned[bestCandidate]++;
            partitions.elementAt(i).assignedFlameWorker = flameWorkers.elementAt(bestCandidate);
        }

        /* Finally, we'll return the partitions to the caller */

        alreadyAssigned = true;
        System.out.println(partitions);
        return partitions;
    }

    public static void main(String args[]) {
        System.out.println(generateSplits(null, null, 9));
    }
}
