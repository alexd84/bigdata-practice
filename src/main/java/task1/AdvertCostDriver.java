package task1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import util.Settings;

import java.net.URI;


public class AdvertCostDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("missing options <input dir> <output dir>\n");
            System.exit(1);
        }
        FileSystem fileSystem = FileSystem.get(new URI(Settings.HDFS_ROOT), new Configuration());
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        fileSystem.delete(outputPath, true);

        Job job = Job.getInstance(getConf(), "task1");
        job.setJarByClass(AdvertCostDriver.class);

        job.setMapperClass(AdvertCostMapper.class);
        job.setReducerClass(AdvertCostReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new AdvertCostDriver(), args));
    }
}
