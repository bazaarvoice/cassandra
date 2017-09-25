package org.apache.cassandra.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.cassandra.dht.ByteOrderedPartitioner;

import java.util.Map;
import java.util.Set;

/**
 * The EmoDB team took ownership of the partitioner used by the service.  EmoPartitioner is functionally equivalent
 * to ByteOrderedPartitioner in all meaningful ways.  As of this writing the only difference is in the computation of
 * ownership:  ByteOrderedPartitioner counts splits to determine ownership while EmoPartitioner assigns ownership based
 * on comparative token range sizes, much like Murmur3Partitioner.
 *
 * Prior to the addition of EmoPartitioner EmoDB used ByteOrderedPartitioner.  To allow for a smooth migration from
 * ByteOrderedPartitioner to EmoPartitioner this class returns that the two partitioners are equivalent.  The primary
 * purpose for this is to allow gossip between a hybrid ring with the two partitioners while nodes with the former
 * partitioner are replaced by those with the latter.  Because the tokens produced by each ring are functionally
 * identical normal operations such as quorum reads, writes, and read repair have all been shown to work in this hybrid
 * state.  In theory it is possible that other operations are also possible in a hybrid state, such as repair jobs
 * and new node joining and bootstrapping these have not been tested and should be avoided while migrating the ring
 * to use EmoPartitioner.
 */
public class EmoPartitioners {
    private static final String EMO_PARTITIONER = "com.bazaarvoice.emodb.partitioner.EmoPartitioner";

    private static final Map<String, Set<String>> EQUIVALENCES = buildEquivalences();

    private static Map<String, Set<String>> buildEquivalences() {
        Set<String> emoPartitionerEquivalenceSet = ImmutableSet.of(ByteOrderedPartitioner.class.getName(), EMO_PARTITIONER);

        ImmutableMap.Builder<String, Set<String>> equivalenceMap = ImmutableMap.builder();

        for (String partitioner : emoPartitionerEquivalenceSet) {
            equivalenceMap.put(partitioner, emoPartitionerEquivalenceSet);
        }

        return equivalenceMap.build();
    }

    public static boolean areEquivalent(String partitioner1, String partitioner2) {
        if (partitioner1.equals(partitioner2)) {
            return true;
        }
        Set<String> equivalences = EQUIVALENCES.get(partitioner1);
        return equivalences != null && equivalences.contains(partitioner2);
    }
}
