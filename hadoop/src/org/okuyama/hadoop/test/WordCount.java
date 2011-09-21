package org.okuyama.hadoop.test;
import java.util.*;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.okuyama.hadoop.format.*;
import org.okuyama.hadoop.config.*;


/**
 * Test Word Count.<br>
 *
 * @author T.Okuyama
 * @license GPLv3
 */
public class WordCount extends Configured implements Tool {


    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new WordCount(), args);
        System.out.println(ret);
    }


    public int run(String[] args) throws Exception {

        Cluster cluster = new Cluster(getConf());

        Job job = Job.getInstance(cluster, getConf());
        job.setJobName("wordcount");
        job.setJarByClass(getClass());

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);

        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        OkuyamaHadoopFormatConfigure.init();
        String[] masterNodes = {"127.0.0.1:8888"};
        OkuyamaHadoopFormatConfigure.setMasterNodeInfoList(masterNodes);
        OkuyamaHadoopFormatConfigure.addTag("tag1");
        OkuyamaHadoopFormatConfigure.addTag("tag2");
        OkuyamaHadoopFormatConfigure.addTag("tag3");
        OkuyamaHadoopFormatConfigure.addTag("tag4");

        job.setInputFormatClass(OkuyamaInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(args[0]));

        // 完了まで待機
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
   }


    public static class Map extends Mapper<String, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        protected void map(String key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, one);
            }
        }
    }


    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println("Key========================================================================[" + key + "]");
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);

        }
    }
}