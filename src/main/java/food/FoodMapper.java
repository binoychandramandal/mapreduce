package food;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class FoodMapper  extends Mapper<LongWritable, Text, Text, Text>
{
	//JXJY167254JK,18-06-2017,654S654,Pizza:Manchurian:Chow Mein:Crispy Onion Rings,197,Wallet,Emperial,08:31:21,2,Late delivery
	@Override
    protected void map(LongWritable key, Text value, Context c)	throws IOException, java.lang.InterruptedException
    {

	String line = value.toString();	//JXJY167254JK,18-06-2017,654S654,Pizza:Manchurian:Chow Mein:Crispy Onion Rings,197,Wallet,Emperial,08:31:21,2,Late delivery

	/* Split csv string */
	String[] words = line.split(",");

	String custID=  (words[0]); 
	
	String data=   (words[1] + "," + words[8] + "," +words[9]);   //  18-06-2017 , 2 , Late delivery
	
	c.write(new Text(custID), new Text(data));
	        // JXJY167254JK      18-06-2017,2,Late delivery
    }
}
