package chain_job;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Job2Mapper  extends Mapper<LongWritable, Text, Text, IntWritable>
{

    @Override
    protected void map(LongWritable key, Text value, Context c)throws IOException, java.lang.InterruptedException
    {

	String[] words = value.toString().split("\\s+");
	/* Job2 mapper, reads in <word, frequency> and emits first character to each word */
	String firstCharacter = words[0].substring(0, 1);
	c.write(new Text(firstCharacter), new IntWritable(Integer.parseInt(words[1])));
	           //    k                   24
	
    }
}