// import necessary libraries
import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

public class secondMapReduce {
	
	// Map Function
		public static class Map extends Mapper<LongWritable, Text, Text, Text>{
			public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
				// input: key=station,year value=avgtemp,avghumidity
				
				String line = values.toString();
				String[] row = line.split("\t");
				
				String strKey = row[0];
				String[] row2 = strKey.split(",");
				String newStrKey = row2[0];
				
				// write output as key=station value=avgtemp, avghumidity
				context.write(new Text(newStrKey), new Text(row[1]));
			}
		}
		// Reduce Function
		public static class Reduce extends Reducer<Text, Text, Text, Text>{
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
				
				int count = 0;
				double temp = 0;
				double humidity = 0;
				
				for(Text x:values) {
					String line = x.toString();
					String[] row = line.split(",");
					count++;
					temp = temp + Double.parseDouble(row[0]);
					humidity = humidity + Double.parseDouble(row[1]);
				}
				double avgTemp = temp/count;
				double avgHumidity = humidity/count;
				String strValue = avgTemp + "," + avgHumidity;
				context.write(key, new Text(strValue));
				
			}
		}
		// Driver function
		public static void main(String[] args) throws Exception{
			Configuration conf = new Configuration();
			Job job = new Job(conf, "Second Map Reduce");
			job.setJarByClass(secondMapReduce.class);
			job.setMapperClass(Map.class);
			job.setCombinerClass(Reduce.class);
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			Path outputPath = new Path(args[1]);
			//Configuring the input/output path from the filesystem into the job
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			//deleting the output path automatically from HDFS so that we don't have to delete it explicitly
			outputPath.getFileSystem(conf).delete(outputPath);
			//exiting the job only if the flag value becomes false
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}

}
