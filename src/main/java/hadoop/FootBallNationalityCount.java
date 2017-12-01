package hadoop;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * футбольний датасет
 * задача - посортувати по громадянству(колонка "nationality") футболістів
 */

public class FootBallNationalityCount extends Job {

    private void createInput(Configuration conf, Path inDir) throws IOException {
        Path infile = new Path(inDir, "text.txt");
        OutputStream os = FileSystem.get(conf).create(infile);
        Writer wr = new OutputStreamWriter(os);
        /*
          Consider a word count input file, where the first line of the file
          contains the words to use in the count.
          COUNT=dog,cat
          My dog is not a cat
          my dog is not a giraffe
          my dog is my freind!
          The outputs of a word count for this job would be:
          dog,3
          cat,1
          In order to implement this sort of wordount, we need non splittable inputs, that is,
          inputs which process headers independently.
         */
        wr.write("dog,cat\n");
        wr.write("My dog is not a cat\n");
        wr.write("My dog is not a cat\n");
        wr.write("my dog is not a giraffe\n");
        wr.write("my dog is my freind!\n");
        wr.close();
        System.out.println("DONE WRITING " + infile);
    }

    static class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);

        protected void map(Text offset, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tok = new StringTokenizer(value.toString(), " ");
            while (tok.hasMoreTokens()) {
                Text word = new Text(tok.nextToken());
                context.write(word, ONE);
            }
        }
    }

     static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

     static class HeaderInputFormat extends FileInputFormat<Text, Text> {
        Text header;
        Set<String> wordsToCount = new HashSet<>();

        /**
         * We dont split files when files have headers.  We need to read the
         * header in the init method, and then use that meta data in the header
         * for counting all words in the file.
         */
        @Override
        protected boolean isSplitable(JobContext context, Path file) {
            return false;
        }

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
            return new KeyValueLineRecordReader(arg1.getConfiguration()) {
                @Override
                public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
                    super.initialize(genericSplit, context);
                    /*
                     * Read the first line.
                     */
                    super.nextKeyValue();
                    header = super.getCurrentKey();
                    System.out.println("header = " + header);
                    if (header.toString().length() == 0)
                        throw new RuntimeException("No header :(");
                    String[] words = header.toString().split(",");
                    for (String s : words) {
                        wordsToCount.add(s);
                        System.out.println("COUNTING " + s);
                    }
                    System.out.println("\nCOUNT ::: " + wordsToCount);
                }

                /**
                 * In this case, we'll just return the same thing
                 * for both the key and value.  A little strange but..
                 * its a good way to demonstrate that the RecordReader
                 * decouples us from the default implementation of key/value
                 * parsing... which is the point of this post anyways :)
                 */
                @Override
                public Text getCurrentKey() {
                    // TODO Auto-generated method stub
                    return this.getCurrentValue();
                }

                /**
                 * Starts at line 2.  First line is the header.
                 */
                @Override
                public Text getCurrentValue() {
                    String raw = super.getCurrentKey().toString();
                    String[] fields = raw.split(" ");
                    String filteredLine = "";
                    /*
                     * Use the metadata in the header
                     * to filter out strings which we're not interested in.
                     */
                    for (String f : fields) {
                        if (wordsToCount.contains(f)) {
                            filteredLine += f + " ";
                        }
                    }
                    System.out.println("RECORD READER FILTERED LINE USING HEADER META DATA: " + filteredLine);
                    return new Text(filteredLine);
                }

            };
        }
    }

    public FootBallNationalityCount(Configuration conf) throws IOException {
        try {
            Path inDir = new Path("inputSJWH");
            Path outDir = new Path("outputSJWH");
            FileSystem.get(this.getConfiguration()).delete(inDir, true);
            FileSystem.get(this.getConfiguration()).delete(outDir, true);
            createInput(this.getConfiguration(), inDir);
            setJobName("MapReduce example for scientific data with headers");
            setOutputKeyClass(Text.class);
            setOutputValueClass(IntWritable.class);
            setInputFormatClass(HeaderInputFormat.class);
            setMapperClass(WordCountMapper.class);
            setReducerClass(IntSumReducer.class);
            FileInputFormat.addInputPath(this, inDir);
            FileOutputFormat.setOutputPath(this, outDir);
            Assert.assertTrue(this.waitForCompletion(true));
            // Check the output is as expected
            FileStatus[] outputFiles = FileSystem.get(this.getConfiguration())
                    .listStatus(outDir);
            for (FileStatus file : outputFiles) {
                System.out.println(file.getPath() + " " + file.getLen());
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    public static void main(String[] args) {
        try {
            new FootBallNationalityCount(new Configuration());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}