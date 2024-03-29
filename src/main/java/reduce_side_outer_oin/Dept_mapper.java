package reduce_side_outer_oin;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Dept_mapper extends Mapper<LongWritable, Text, Text, Text>
{
        //  10,Inventory,HYDERABAD

        public void map(LongWritable key, Text V1, Context context) throws IOException,InterruptedException
        {

                String line = V1.toString().trim();      //  line =   10,Inventory,HYDERABAD

                String[] Temparray = line.split(",");       // Temparray = [{10} {Inventory} {HYDERABAD}]

                context.write(new Text(Temparray[0]), new Text("department,"+Temparray[1]+" "+Temparray[2]));
        }
}