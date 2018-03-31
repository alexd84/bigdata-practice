package task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class GroupMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        JSONObject json = new JSONObject(value.toString());

        JSONObject publisherGroup = new JSONObject();
        publisherGroup.put("id", json.getInt("id"));
        publisherGroup.put("date", json.getString("date"));

        context.write(new Text(publisherGroup.toString()), value);
    }
}
