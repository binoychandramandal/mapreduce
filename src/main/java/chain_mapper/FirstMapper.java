package chain_mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{

    private static final IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	/* First Mapper reads in each line, split it into words and emit every word */
	String[] words = value.toString().split(",");

	for (String word : words)
	{
	    c.write(new Text(word), ONE);
	}
    }
}