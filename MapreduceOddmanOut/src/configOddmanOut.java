import java.io.IOException;

import javax.naming.ConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils.Text;


public class configOddmanOut {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//create configuration
Configuration conf=new Configuration(true);
		//create job
Job job= new Job(conf, "Odd-man Out");
	job.setJarByClass(oddmanMapper.class);

//MapReduce
job.setMapperClass(oddmanMapper.class);
job.setReducerClass(oddmanReducer.class);
job.setNumReduceTasks(1);
//specify key and value
job.setOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
//input & output paths
Path inputPath=new Path(args[0]);
Path outputPath=new Path(args[1]);
//input 

	FileInputFormat.addInputPath(job, inputPath);

job.setInputFormatClass(TextInputFormat.class);
//output
FileOutputFormat.setOutputPath(job, outputPath);
job.setOutputFormatClass(TextOutputFormat.class);
//delete output if exists
FileSystem hdfs;

	hdfs = FileSystem.get(conf);

try {
	if (hdfs.exists(outputPath))
		hdfs.delete(outputPath, true);
} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}

//execute job

	int code = job.waitForCompletion(true)? 0 : 1;

System.exit(code);


	}

}
