package reduce_side_outer_oin;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OuterJoin_Driver
{
        public static void main(String[] args) throws IOException,ClassNotFoundException, InterruptedException
        {

                Path employeeFilePath = new Path(args[0]);
                Path deptFilePath = new Path(args[1]);

                Path OutputDirectory = new Path(args[2]);

                Configuration C1 = new Configuration();

                Job job = new Job(C1, "OuterJoin");
                // name of driver class
                job.setJarByClass(OuterJoin_Driver.class);
                //name of Mapper1 class
                job.setMapperClass(Emp_Mapper.class);
                // name of Mapper2 class
                job.setMapperClass(Dept_mapper.class);
                // name of Reducer class
                job.setReducerClass(OuterJoin_reducer.class);

                job.setMapOutputKeyClass(Text.class);
                job.setMapOutputValueClass(Text.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, employeeFilePath, TextInputFormat.class,Emp_Mapper.class);   // employee file will processed by Emp_Mapper

                MultipleInputs.addInputPath(job, deptFilePath, TextInputFormat.class, Dept_mapper.class);    // dept file will processed by Dep_Mapper


                FileOutputFormat.setOutputPath(job, OutputDirectory);
                OutputDirectory.getFileSystem(job.getConfiguration()).delete(OutputDirectory,true);

                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
