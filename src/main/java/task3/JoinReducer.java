package task3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class JoinReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // you can create custom objects
        JSONObject publisherData = new JSONObject();
        publisherData.put("publisherId", 123);
        publisherData.put("date", "01-01-2015");

        context.write(null, null);
    }
}
