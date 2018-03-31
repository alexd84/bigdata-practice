package task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class JoinMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    private Text auctionId = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());
        auctionId.set(json.getString("auctionId"));
        context.write(auctionId, value);
    }
}
