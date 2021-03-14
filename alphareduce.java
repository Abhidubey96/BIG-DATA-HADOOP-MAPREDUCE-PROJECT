//Abhishek DUbey

//Reducer Class

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class alphareduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
throws IOException, InterruptedException {
//initializing variables for count and total
double alphaCount = 0;
int total = 0;
for (DoubleWritable value : values) {
	alphaCount += value.get();
	total += 1;
}
//Average Count of Alphabets

context.write(key, new DoubleWritable(Math.round((alphaCount/total)*100.0)/100.0));

//Ending Reducing Class
}

}