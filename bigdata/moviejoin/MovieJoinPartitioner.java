package bigdata.moviejoin;

/**
 * Created by ypwen on 5/5/2017.
 */
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MovieJoinPartitioner extends Partitioner<IntTextWritable, Text> {

    public int getPartition(IntTextWritable key, Text value, int numOfReduceTasks) {
        return key.getMovieId().get() % numOfReduceTasks;
    }
}