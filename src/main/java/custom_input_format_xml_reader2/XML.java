package custom_input_format_xml_reader2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class XML
{

    public void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {

	 Path inputPath = new Path(args[0]);
	 Path outputDir = new Path(args[1]);

	Configuration conf = new Configuration();
	Job job = new Job(conf, "XML Reader");
	
	job.setInputFormatClass(XMLInputFormat.class);  //set your own input format class
	// name of driver class
	job.setJarByClass(XML.class);
	// name of mapper class
	job.setMapperClass(XMLMapper.class);
	
	job.setOutputKeyClass(LongWritable.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputDir);
	
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	job.waitForCompletion(true);
    }
}
