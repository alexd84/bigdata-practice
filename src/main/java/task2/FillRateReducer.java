package task2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class FillRateReducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, FloatWritable> {
    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(null, null);
    }
}
