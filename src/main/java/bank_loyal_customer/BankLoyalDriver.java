package bank_loyal_customer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BankLoyalDriver{

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
    	Path accountPath = new Path("hdfs://localhost:9000/user/jivesh/account");
		Path personPath = new Path("hdfs://localhost:9000/user/jivesh/person");
		Path outputDir = new Path("hdfs://localhost:9000/user/jivesh/output");

	Configuration conf = new Configuration();
	Job job = new Job(conf, "Bank Customer Loyality");
	job.setJarByClass(BankLoyalDriver.class);
	job.setMapperClass(AccountMapper.class);
	job.setMapperClass(PersonMapper.class);
	job.setReducerClass(BankLoyalReducer.class);

	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(Text.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	MultipleInputs.addInputPath(job, accountPath,  TextInputFormat.class,  AccountMapper.class);

	MultipleInputs.addInputPath(job,  personPath, TextInputFormat.class, PersonMapper.class);

	FileOutputFormat.setOutputPath(job, outputDir);
	outputDir.getFileSystem(job.getConfiguration()).delete(outputDir,true);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
