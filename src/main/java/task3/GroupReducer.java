package task3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class GroupReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text publisherGroup, Iterable<Text> jsonObjects, Context context) throws IOException, InterruptedException {
        JSONObject publisherGroupJson = new JSONObject(publisherGroup.toString());
        int publisherId = publisherGroupJson.getInt("id");
        String date = publisherGroupJson.getString("date");

        int req = 0;
        int rsp = 0;
        int imp = 0;
        for (Text jsonObj : jsonObjects) {
            JSONObject json = new JSONObject(jsonObj.toString());
            req += json.getInt("req");
            rsp += json.getInt("rsp");
            imp += json.getInt("imp");
        }

        JSONObject publisherData = new JSONObject();
        publisherData.put("publisherId", publisherId);
        publisherData.put("date", date);
        publisherData.put("req", req);
        publisherData.put("rsp", rsp);
        publisherData.put("imp", imp);

        context.write(NullWritable.get(), new Text(publisherData.toString()));
    }
}
