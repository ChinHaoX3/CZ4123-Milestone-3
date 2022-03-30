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

// first mapreduce
// map input: key=index, value=station,timestamp,tmp,humidity
// map output: key=station,year value=tmp,humidity
public class firstMapReduce {
	
	// Map Function
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Create format for date time
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
			
			// read csv files
			String line = value.toString();
			// split the line
			String[] row = line.split(",");
			// assign key - station,year
			
			if(!row[2].equals("M") && !row[3].equals("M")) {
				LocalDateTime datetime = LocalDateTime.parse(row[1], formatter);
				String strKey = row[0] + "," + datetime.getYear();
				// int strKey = Integer.parseInt(row[0]);
				// temperature and humidity
				String strValue = row[2] + "," + row[3];
				context.write(new Text(strKey), new Text(strValue));
			}
		}
	}
	// Reduce Function
	public static class Reduce extends Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			
			// key is station,month,year
			
			// value is list inside list
			int count = 0;
			double temp = 0;
			double humidity = 0;
			
			//ArrayList<String> valuesList = new ArrayList<>();
			for(Text x:values) {
				// [temperature, humidity]
				String line = x.toString();
				String[] row = line.split(",");
				count ++;
				temp = temp + Double.parseDouble(row[0]);
				humidity = humidity + Double.parseDouble(row[1]);
				// valuesList.add(x.toString());
			}
			double avgTemp = temp/count;
			double avgHumidity = humidity/count;
			String strValue = avgTemp + "," + avgHumidity;
			
			context.write(key, new Text(strValue));
			
			//context.write(key, new Text(valuesList.toString()));
		}
	}
	// Driver function
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "First Map Reduce");
		job.setJarByClass(firstMapReduce.class);
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

