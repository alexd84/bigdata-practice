package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class FillRateMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, Text> {
    private IntWritable advertId = new IntWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());
        advertId.set(json.getInt("advertiserId"));
        context.write(advertId, value);
    }
}
