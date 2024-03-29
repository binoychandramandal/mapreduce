package sequence_file;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
public class SequenceFileDriver
{
    public static void main(String[] args) throws IOException,  ClassNotFoundException,  InterruptedException
    {
		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

	Configuration conf = new Configuration();
	Job job = new Job(conf, "");

	job.setJarByClass(SequenceFileDriver.class);

	job.setMapperClass(SequenceFileMapper.class);
	job.setReducerClass(SequenceFileReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	/* set job to read in a sequence file instead of a text file */
	job.setInputFormatClass(SequenceFileInputFormat.class);
	/* OPTIMIZATIONS */
	/* Optimization-1: Work with binary sequence files for efficiency */
	
	job.setOutputFormatClass(SequenceFileOutputFormat.class);
	
	/* Optimization-2: Compress sequence file */
	
	FileOutputFormat.setCompressOutput(job, true);
	FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

	FileInputFormat.addInputPath(job, inputPath);
	
	FileOutputFormat.setOutputPath(job, outputDir);
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	job.waitForCompletion(true);
	
	
    }
}