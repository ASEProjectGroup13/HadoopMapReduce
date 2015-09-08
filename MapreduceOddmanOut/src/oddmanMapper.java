import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class oddmanMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	IntWritable inte=new IntWritable();
	Text key1=new Text();

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String s1[]= value.toString().split(" ");
		key1.set( "1");
		for(String st:s1)
		{
			inte.set(Integer.parseInt(st));
			context.write(key1, inte);
		}
		
	}
	

	
}
