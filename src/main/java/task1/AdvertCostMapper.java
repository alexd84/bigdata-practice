package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.json.*;

class AdvertCostMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, LongWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());

        // you can read json fields as following
        int advertiserId = json.getInt("advertiserId");
        String date = json.getString("date");

        // write key and value
        context.write(null, null);
    }
}
