package sum_even_odd;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EvenOddDriver
{
    public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException
    {

        Path input_file_loc = new Path(args[0]);   //harcoded input location
        Path output_dir_loc = new Path(args[1]);       //harcoded output location

        Configuration conf = new Configuration();
        Job job = new Job(conf, "Evenodd");

        ////name of Driver class
        job.setJarByClass(EvenOddDriver.class);
        //name of mapper class
        job.setMapperClass(EvenOddMapper.class);
        // name of reducer class
        job.setReducerClass(EvenOddReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input_file_loc);
        FileOutputFormat.setOutputPath(job, output_dir_loc);
        output_dir_loc.getFileSystem(job.getConfiguration()).delete(output_dir_loc,true);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
