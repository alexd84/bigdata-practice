package task3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.json.JSONObject;

import java.io.IOException;

class JoinReducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, NullWritable, Text> {
    public void reduce(Text auctionId, Iterable<Text> jsonObjects, Context context) throws IOException, InterruptedException {
        int req = 0;
        int rsp = 0;
        int imp = 0;
        int publisherId = 0;
        String date = "";

        for (Text jsonObj : jsonObjects) {
            JSONObject json = new JSONObject(jsonObj.toString());
            String type = json.getString("type");
            if (type.equals("AdRequest")) {
                req = 1;
                publisherId = json.getInt("publisherId");
                date = json.getString("date");
            }
            if (type.equals("AdResponse")) {
                rsp += 1;
            }
            if (type.equals("Impression")) {
                imp += 1;
            }
        }

        JSONObject publisherData = new JSONObject();
        publisherData.put("id", publisherId);
        publisherData.put("date", date);
        publisherData.put("req", req);
        publisherData.put("rsp", rsp);
        publisherData.put("imp", imp);

        context.write(NullWritable.get(), new Text(publisherData.toString()));
    }
}
