import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class oddmanReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

@Override
protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	int sum=0;
	IntWritable integ=new IntWritable();
	
	for(IntWritable value: values)
	{
				sum=sum^value.get();
	}
	integ.set(sum);
	context.write(key, integ); 
	
	
}
}
