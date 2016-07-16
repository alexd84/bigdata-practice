package task3;

import util.Settings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;


public class DailyDriver extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("missing options <input dirs> <output dir>\n");
            System.exit(1);
        }

        FileSystem fileSystem = FileSystem.get(new URI(Settings.HDFS_ROOT), new Configuration());
        Path temporaryDataPath = new Path("/user/bigdata/tmp");
        Path outputPath = new Path(args[1]);
        fileSystem.delete(outputPath, true);

        Job job1 = Job.getInstance(getConf(), "task3-phase1");
        job1.setJarByClass(DailyDriver.class);
        job1.setMapperClass(JoinMapper.class);
        job1.setReducerClass(JoinReducer.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPaths(job1, args[0]);
        FileOutputFormat.setOutputPath(job1, temporaryDataPath);
        boolean returnCode1 = job1.waitForCompletion(true);

        Job job2 = Job.getInstance(getConf(), "task3-phase2");
        job2.setJarByClass(DailyDriver.class);
        job2.setMapperClass(GroupMapper.class);
        job2.setReducerClass(GroupReducer.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(NullWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, temporaryDataPath);
        FileOutputFormat.setOutputPath(job2, outputPath);
        boolean returnCode2 = job2.waitForCompletion(true);

        fileSystem.delete(temporaryDataPath, true);

        return returnCode1 && returnCode2 ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        System.exit(ToolRunner.run(conf, new DailyDriver(), args));
    }
}
