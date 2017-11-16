package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;

public class SampleTool extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SampleTool(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        FileSystem hdfs = FileSystem.get(conf);
        Path outDir = new Path("output");
        hdfs.delete(outDir, true);
        if (hdfs.exists(outDir))
            return 2;
        Job job = Job.getInstance(conf, "football nationality count");
        job.setJarByClass(FootBall.class);
        job.setMapperClass(FootBall.FootballMapper.class);
        job.setCombinerClass(FootBall.IntSumReducer.class);
        job.setReducerClass(FootBall.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setSortComparatorClass(FootBall.KeyComparator.class);
        FileInputFormat.addInputPath(job, new Path("epldata_final.csv"));
        FileOutputFormat.setOutputPath(job, outDir);
        Assert.assertTrue(job.waitForCompletion(true));
        FileStatus[] outputFiles = FileSystem.get(job.getConfiguration()).listStatus(outDir);
        for (FileStatus file : outputFiles) {
            System.out.println(file.getPath() + " " + file.getLen());
        }
        return 0;
    }
}
