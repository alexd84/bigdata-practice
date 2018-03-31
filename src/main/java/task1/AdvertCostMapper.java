package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class AdvertCostMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, LongWritable> {
    private IntWritable advertId = new IntWritable();
    private LongWritable cost = new LongWritable();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());
        advertId.set(json.getInt("advertiserId"));
        cost.set(json.getInt("price"));
        context.write(advertId, cost);
    }
}
