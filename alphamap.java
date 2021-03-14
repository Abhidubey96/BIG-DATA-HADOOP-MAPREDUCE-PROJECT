//Abhishek Dubey
//Mapper Class

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
public class alphamap extends Mapper<LongWritable, Text, Text, DoubleWritable> {
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
String s = value.toString();
//putting all text to lowercase
//replacing all special characters 
s = s.toLowerCase();
s = s.replaceAll("[^a-zA-Z]","");

StringBuilder str = new StringBuilder(s);
double count = 0.0;

double strlen = str.length();
//counting all characters in string 
for(int i = 0; i < str.length(); i++){
	if(Character.isLetter(str.charAt(i))){
		count = 1;
		for(int j = i+1; j < str.length(); j++){
		if (str.charAt(i)==str.charAt(j)){
			count += 1.0;
			str.setCharAt(j, ' ');
		}
	}
		
	context.write(new Text(Character.toString(str.charAt(i))), new DoubleWritable(Math.round((count/strlen)*100.0)/100.0));
			
			count = 0;
	
}	
}
//Ending Mapper 1 

}
}