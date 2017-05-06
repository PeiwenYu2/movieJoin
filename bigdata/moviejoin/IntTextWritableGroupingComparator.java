package bigdata.moviejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by ypwen on 5/5/2017.
 */
public class IntTextWritableGroupingComparator extends WritableComparator{

    protected IntTextWritableGroupingComparator() {
        super(IntTextWritable.class, true);
    }
    @Override
    public int compare(WritableComparable o1, WritableComparable o2) {
        return (((IntTextWritable)o1).getMovieId()).compareTo(((IntTextWritable)o2).getMovieId());
    }
}