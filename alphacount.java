//Abhishek Dubey
//Hadoop Driver Class

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class alphacount {
public static void main(String[] args) throws Exception {


Configuration conf = new Configuration();

//job 1 for English Language

conf.set("Lang", "ENGLISH");

Job job = Job.getInstance(conf, "alphacount");
job.setJarByClass(alphacount.class);


Configuration AlphaMapConfig = new Configuration(false);
ChainMapper.addMapper(job, alphamap.class, LongWritable.class, Text.class, Text.class, DoubleWritable.class, AlphaMapConfig);

Configuration KeyMapConfig = new Configuration(false);
ChainMapper.addMapper(job, keymap.class, Text.class, DoubleWritable.class,Text.class, DoubleWritable.class, KeyMapConfig);

job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(DoubleWritable.class);

job.setPartitionerClass(HashPartitioner.class);

job.setReducerClass(alphareduce.class);
//for one output file
job.setNumReduceTasks(1);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(DoubleWritable.class);


FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));

ControlledJob cJob = new ControlledJob(conf);
cJob.setJob(job);

System.out.println("job 1 for English Language");

//job 2 for French Language
conf.set("Lang", "FRENCH");

Job job2 = Job.getInstance(conf, "alphacount");
job2.setJarByClass(alphacount.class);


Configuration AlphaMapConfig2 = new Configuration(false);
ChainMapper.addMapper(job2, alphamap.class, LongWritable.class, Text.class, Text.class, DoubleWritable.class, AlphaMapConfig2);

Configuration KeyMapConfig2 = new Configuration(false);
ChainMapper.addMapper(job2, keymap.class, Text.class, DoubleWritable.class,Text.class, DoubleWritable.class, KeyMapConfig2);


job2.setMapOutputKeyClass(Text.class);
job2.setMapOutputValueClass(DoubleWritable.class);

job2.setPartitionerClass(HashPartitioner.class);

job2.setReducerClass(alphareduce.class);
//for one output file
job2.setNumReduceTasks(1);


job2.setOutputKeyClass(Text.class);
job2.setOutputValueClass(DoubleWritable.class);

FileInputFormat.addInputPath(job2, new Path(args[2]));
FileOutputFormat.setOutputPath(job2, new Path(args[3]));

ControlledJob cJob2 = new ControlledJob(conf);
cJob2.setJob(job2);

System.out.println("job 2 for French Language");
//job 3 for Spanish Language

conf.set("Lang", "SPANISH");

Job job3 = Job.getInstance(conf, "alphacount");
job3.setJarByClass(alphacount.class);


Configuration AlphaMapConfig3 = new Configuration(false);
ChainMapper.addMapper(job3, alphamap.class, LongWritable.class, Text.class, Text.class, DoubleWritable.class, AlphaMapConfig3);

Configuration KeyMapConfig3 = new Configuration(false);
ChainMapper.addMapper(job3, keymap.class, Text.class, DoubleWritable.class,Text.class, DoubleWritable.class, KeyMapConfig3);


job3.setMapOutputKeyClass(Text.class);
job3.setMapOutputValueClass(DoubleWritable.class);

job3.setPartitionerClass(HashPartitioner.class);

job3.setReducerClass(alphareduce.class);
//for one output file
job3.setNumReduceTasks(1);


job3.setOutputKeyClass(Text.class);
job3.setOutputValueClass(DoubleWritable.class);

FileInputFormat.addInputPath(job3, new Path(args[4]));
FileOutputFormat.setOutputPath(job3, new Path(args[5]));

ControlledJob cJob3 = new ControlledJob(conf);
cJob3.setJob(job3);

System.out.println("job 3 for Spanish Language");

//creating job control for all jobs
JobControl jobctrl = new JobControl("jobctrl");
jobctrl.addJob(cJob);
jobctrl.addJob(cJob2);
jobctrl.addJob(cJob3);

Thread jobRunnerThread = new Thread(jobctrl);
jobRunnerThread.start();

//running job control in while loop

while (!jobctrl.allFinished()){
	System.out.println("MapReduce Process is running in background - It will take some time according to the processing power");
	Thread.sleep(5000);
}
jobctrl.stop();
int code = jobctrl.getFailedJobList().size() == 0 ? 0:1;

System.out.println("Output Generated, Thanks for Running Abhishek Dubey Code for Alpha Count");

System.exit(code);

}
}

