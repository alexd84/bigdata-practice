package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

class AdvertCostReducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
    public void reduce(IntWritable advertId, Iterable<LongWritable> costs, Context context) throws IOException, InterruptedException {
        long sum = 0;

        for (LongWritable cost : costs) {
            sum += cost.get();
        }

        context.write(advertId, new LongWritable(sum));
    }
}
