package food;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Food{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {

	 Path inputPath = new Path("hdfs://localhost:9000/user/jivesh/food");
	 Path outputDir = new Path("hdfs://localhost:9000/user/jivesh/output/");

	
	Configuration conf = new Configuration();
	Job job = new Job(conf, "Food Analysis");

	job.setJarByClass(Food.class);
	job.setMapperClass(FoodMapper.class);
	job.setReducerClass(FoodReducer.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	FileInputFormat.addInputPath(job, inputPath);


	FileOutputFormat.setOutputPath(job, outputDir);
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	job.waitForCompletion(true);
    }
}
