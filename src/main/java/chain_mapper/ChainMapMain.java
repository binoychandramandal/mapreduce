package chain_mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ChainMapMain{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Path inputPath = new Path(args[0]);
		Path outputDir = new Path(args[1]);

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Chain Job1");

		job.setJarByClass(ChainMapMain.class);
		/* add multiple chained mappers to run for this job */
		ChainMapper.addMapper(job,
				FirstMapper.class, /* Mapper to add to chain */
				LongWritable.class, /* Mapper input Key */
				Text.class, /* Mapper input value */
				Text.class, /* Mapper output key*/
				IntWritable.class, /* Mapper output value */
				conf /* job configuration to use */
		);

		/* set second mapper in the chain */
		ChainMapper.addMapper(job,
				SecondMapper.class, /* Mapper to add to chain */
				Text.class, /* Mapper input Key */
				IntWritable.class, /* Mapper input value */
				Text.class, /* Mapper output key*/
				IntWritable.class, /* Mapper output value */
				conf /* job configuration to use */
		);

		/* set reducer for the chain */
		ChainReducer.setReducer(job,
				CMReducer.class, /* Reducer to add to chain */
				Text.class, /* Reducer input Key */
				IntWritable.class, /* Reducer input value */
				Text.class, /* Reducer output key*/
				IntWritable.class, /* Reducer output value */
				conf /* job configuration to use */
		);
		FileInputFormat.addInputPath(job, inputPath);

		FileOutputFormat.setOutputPath(job, new Path(outputDir + "_job1"));
		outputDir.getFileSystem(job.getConfiguration()).delete(new Path(outputDir + "_job1"), true);
  System.out.println(job.waitForCompletion(true));
	}

}