import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<Object> {

        Text first;
        Text second;

        public TextPair() {}

        public TextPair(Text t1, Text t2) {
                first = t1;
                second = t2;
        }
       
        public void setFirst(String t1) {
                first.set(t1.getBytes());
        }
       
        public void setSecond(String t2) {
                second.set(t2.getBytes());
        }

        public Text getFirst() {
                return first;
        }

        public Text getSecond() {
                return second;
        }

        public void write(DataOutput out) throws IOException {
                first.write(out);
                second.write(out);
        }

        public void readFields(DataInput in) throws IOException {
                if (first == null)
                        first = new Text();

                if (second == null)
                        second = new Text();

                first.readFields(in);
                second.readFields(in);
        }

        public int compareTo(Object object) {
                TextPair tp2 = (TextPair) object;
                int cmp = getFirst().compareTo(tp2.getFirst());
                if (cmp != 0) {
                        return cmp;
                }
                return getSecond().compareTo(tp2.getSecond()); // reverse
        }

        public int hashCode() {
                return first.hashCode();
        }

        public boolean equals(Object o) {
                TextPair p = (TextPair) o;
                return first.equals(p.getFirst());
        }
}

