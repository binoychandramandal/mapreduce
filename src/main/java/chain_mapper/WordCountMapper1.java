package chain_mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper1 extends Mapper<Text, IntWritable, Text, IntWritable>{
public void map(Text key, IntWritable value, Context con) throws IOException, InterruptedException
        {
        String line = value.toString().toLowerCase();

        con.write(new Text(line), value);

        }
        }