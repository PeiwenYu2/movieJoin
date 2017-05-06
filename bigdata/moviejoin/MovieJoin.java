package bigdata.moviejoin;

/**
 * Created by ypwen on 5/5/2017.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieJoin {

    public static class MovieMapper extends Mapper<LongWritable, Text, IntTextWritable, Text> {

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntTextWritable, Text>.Context context) throws IOException, InterruptedException {
            if(value != null) {
                String source = value.toString().trim();
                if(source.length() != 0) {
                    String[] columns = source.split("\\|");
                    System.out.println("1" + columns[1]);
                    context.write(new IntTextWritable(new IntWritable(Integer.parseInt(columns[0])), new Text("movieMapper")), new Text(columns[1]));
                }
            }
        }
    }

    public static class RateMapper extends Mapper<LongWritable, Text, IntTextWritable, Text> {

        public void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntTextWritable, Text>.Context context) throws IOException, InterruptedException {
            if(value != null) {
                String source = value.toString().trim();
                if(source.length() != 0) {
                    String[] columns = source.split("\\t");
                    StringBuilder sb = new StringBuilder();
                    sb.append(columns[0]).append(" : ").append(columns[2]);
                    context.write(new IntTextWritable(new IntWritable(Integer.parseInt(columns[1])), new Text("rateMapper")), new Text(sb.toString()));
                }
            }
        }
    }

    public static class MovieJoinReducer extends Reducer<IntTextWritable, Text, IntWritable, Text> {

        public void reduce(IntTextWritable key, Iterable<Text> values, Reducer<IntTextWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {
            String movieName = null;
            System.out.println(key.getMapperName().toString());
            for (Text value : values) {
                if(key.getMapperName().toString().equals("movieMapper")) {
                    movieName = value.toString();
                    System.out.println("2" + movieName);
                } else {
                    StringBuilder sb = new StringBuilder();
                    sb.append(movieName).append(" - ").append(value.toString());
                    context.write(key.getMovieId(), new Text(sb.toString()));
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        if(args.length < 3) {
            System.err.println("Input and output directories are needed");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MovieJoin");
        job.setJarByClass(MovieJoin.class);
        job.setMapperClass(MovieMapper.class);
        job.setMapperClass(RateMapper.class);
        job.setMapOutputKeyClass(IntTextWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setPartitionerClass(MovieJoinPartitioner.class);
        job.setGroupingComparatorClass(IntTextWritableGroupingComparator.class);

        job.setReducerClass(MovieJoinReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RateMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true)?1:0);

    }
}