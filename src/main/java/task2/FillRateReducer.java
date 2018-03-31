package task2;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class FillRateReducer extends org.apache.hadoop.mapreduce.Reducer<IntWritable, Text, IntWritable, FloatWritable> {
    public void reduce(IntWritable advertId, Iterable<Text> jsonObjects, Context context) throws IOException, InterruptedException {
        long sumRequests = 0;
        long sumResponses = 0;

        for (Text jsonObj : jsonObjects) {
            JSONObject json = new JSONObject(jsonObj.toString());
            String type = json.getString("type");
            if (type.equals("AdRequest")) {
                sumRequests += 1;
            }
            if (type.equals("AdResponse")) {
                sumResponses += 1;
            }
        }

        float fillRate = (float) sumResponses / sumRequests;

        context.write(advertId, new FloatWritable(fillRate));
    }
}
