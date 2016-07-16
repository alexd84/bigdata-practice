package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer iterator = new StringTokenizer(value.toString(), " ");

        while (iterator.hasMoreElements()) {
            String token = iterator.nextToken();
            context.write(new Text(token), new IntWritable(1));
        }
    }
}
