package chain_mapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.tools.ant.util.ChainedMapper;

public class ChainMapperDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf,"WordCount");
        ChainMapper.addMapper(job,WordCountMapper.class, LongWritable.class,Text.class,Text.class,IntWritable.class,conf);
        ChainMapper.addMapper(job,WordCountMapper1.class,Text.class, IntWritable.class,Text.class,IntWritable.class,conf);
        ChainReducer.setReducer(job,WordCountReducer.class,Text.class,IntWritable.class,Text.class,IntWritable.class,conf);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.out.println(job.waitForCompletion(true)?0:1);
    }
}
