package sequence_file;
/* Hadoop imports */
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.conf.Configuration;

public class SequenceFileReader
{
    public static void main(String[] args) throws Exception 
    {
	Configuration conf = new Configuration();
	Path path = new Path(args[0]);
	Reader reader = null;
	try {
	    reader = new Reader(FileSystem.get(conf), path,conf);
	    
	    Text key = new Text();
	    //Text value = new Text();
	    IntWritable value = new IntWritable();
	    
	    while (reader.next(key, value))
	    {
		System.out.println(key + ", " + value.toString());
	    }
	} catch(Exception e){e.printStackTrace();}
    }
}