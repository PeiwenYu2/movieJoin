package bigdata.moviejoin;

/**
 * Created by ypwen on 5/5/2017.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class IntTextWritable implements WritableComparable<IntTextWritable> {
    private final IntWritable movieId;
    private final Text mapperName;

    public IntTextWritable() {
        this.movieId = new IntWritable(0);
        this.mapperName = new Text("");
    }

    public IntTextWritable(IntWritable movieId, Text mapperName) {
        this.movieId = movieId;
        this.mapperName = mapperName;
    }

    public int compareTo(IntTextWritable o) {
        if (o == null) {
            return 0;
        }
        return this.movieId.equals(o.movieId)?this.mapperName.compareTo(o.mapperName):this.movieId.compareTo(o.movieId);
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.movieId.write(dataOutput);
        this.mapperName.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.movieId.readFields(dataInput);
        this.mapperName.readFields(dataInput);
    }

    public IntWritable getMovieId() {
        return this.movieId;
    }

    public Text getMapperName() {
        return this.mapperName;
    }
}
