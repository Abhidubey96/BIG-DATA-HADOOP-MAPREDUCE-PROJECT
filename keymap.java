//Abhishek Dubey
//key map with values

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class keymap extends Mapper<Text, DoubleWritable, Text, DoubleWritable> {
public void map(Text key, DoubleWritable value, Context context)
throws IOException, InterruptedException {
	Configuration conf = context.getConfiguration();
	String langCode = conf.get("Lang");
//extracting output file in language + key + value format
	context.write(new Text(langCode+"\t"+"\t"+key), value);
	
//Ending Mapper 2	
}
}