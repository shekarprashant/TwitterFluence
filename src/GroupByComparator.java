import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class GroupByComparator extends WritableComparator {
        protected GroupByComparator() {
                super(TextPair.class, true);
        }

        @SuppressWarnings("rawtypes")
		@Override
        public int compare(WritableComparable w1, WritableComparable w2) {
                TextPair ip1 = (TextPair) w1;
                TextPair ip2 = (TextPair) w2;
                return ip1.getFirst().compareTo(ip2.getFirst());
        }
}
