package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

class AdvertCostReducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
    public void reduce(IntWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        context.write(null, null);
    }
}
