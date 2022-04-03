
// import necessary libraries
import java.lang.*;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.Path;

public class WeatherMapReduce {

	// initialisation
	public static ArrayList<String> oldCentroids = new ArrayList<String>();
	public static ArrayList<String> newCentroids = new ArrayList<String>();
	public static double threshold = 0.001;
	public static int k = 4;

	// First Map Function
	public static class FirstMap extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Create format for date time
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

			// read csv files
			String line = value.toString();
			// split the line
			String[] row = line.split(",");
			// assign key - station,year

			if (!row[2].equals("M") && !row[3].equals("M")) {
				LocalDateTime datetime = LocalDateTime.parse(row[1], formatter);
				String strKey = row[0] + "," + datetime.getYear();
				// int strKey = Integer.parseInt(row[0]);
				// temperature and humidity
				String strValue = row[2] + "," + row[3];
				context.write(new Text(strKey), new Text(strValue));
			}
		}
	}

	// First Reduce Function
	public static class FirstReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// key is station,month,year

			// value is list inside list
			int count = 0;
			double temp = 0;
			double humidity = 0;

			// ArrayList<String> valuesList = new ArrayList<>();
			for (Text x : values) {
				// [temperature, humidity]
				String line = x.toString();
				String[] row = line.split(",");
				count++;
				temp = temp + Double.parseDouble(row[0]);
				humidity = humidity + Double.parseDouble(row[1]);
				// valuesList.add(x.toString());
			}
			double avgTemp = temp / count;
			double avgHumidity = humidity / count;
			String strValue = avgTemp + "," + avgHumidity;

			context.write(key, new Text(strValue));

			// context.write(key, new Text(valuesList.toString()));
		}
	}

	// Second Map Function
	public static class SecondMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input: key=station,year value=avgtemp,avghumidity

//			String line = values.toString();
//			String[] row = line.split("\t");
//
//			String strKey = row[0];
//			String[] row2 = strKey.split(",");
//			String newStrKey = row2[0];
//
//			// write output as key=station value=avgtemp, avghumidity
//			context.write(new Text(newStrKey), new Text(row[1]));

			// assume key and value is set nicely

			String line = key.toString();
			String[] row = line.split(",");
			String newStrKey = row[0];

			context.write(new Text(newStrKey), values);
		}
	}

	// Reduce Function
	public static class SecondReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int count = 0;
			double temp = 0;
			double humidity = 0;

			for (Text x : values) {
				String line = x.toString();
				String[] row = line.split(",");
				count++;
				temp = temp + Double.parseDouble(row[0]);
				humidity = humidity + Double.parseDouble(row[1]);
			}
			double avgTemp = temp / count;
			double avgHumidity = humidity / count;
			String strValue = avgTemp + "," + avgHumidity;
			context.write(key, new Text(strValue));

		}
	}

	// Kmeans Map Function - assign clusters
	public static class KMeansMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input key = station, values = temperature, humidity
			// output key = index, values = station,temperature,humidity

			// split values to temperature and humidity
			String stationValues[] = values.toString().split(",");

			// initialise variable
			double curDistance = 0;

			// initialise for index 0
			Configuration conf = context.getConfiguration();

			String[] centroidOne = conf.get("oldcentroid:0").split(",");

			double tempDistance = Math.abs(Double.parseDouble(centroidOne[0]) - Double.parseDouble(stationValues[0]));
			double humidityDistance = Math
					.abs(Double.parseDouble(centroidOne[1]) - Double.parseDouble(stationValues[1]));

			double minDistance = tempDistance + humidityDistance;

			int kmeansIndex = 0;

			// for loop to check assign clusters
			for (int i = 1; i < k; i++) {
				String centroidValues[] = conf.get("oldcentroid:" + i).split(",");
				tempDistance = Math.abs(Double.parseDouble(centroidValues[0]) - Double.parseDouble(stationValues[0]));
				humidityDistance = Math
						.abs(Double.parseDouble(centroidValues[1]) - Double.parseDouble(stationValues[1]));
				curDistance = tempDistance + humidityDistance;

				if (curDistance < minDistance) {
					kmeansIndex = i;
					minDistance = curDistance;
				}

			}
			String strValues = key.toString() + "," + stationValues[0] + "," + stationValues[1];
			System.out.println(strValues);
			
			context.write(new Text(Integer.toString(kmeansIndex)), new Text(strValues));
		}
	}

	// Kmeans Reduce Function - recompute centroids values
	public static class KMeansReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input key = index, iterable values = station,temperature,humidity
			// output key = station, values = temperature, humidity
			double allTemp = 0;
			double allHumidity = 0;
			int count = 0;
			
			for (Text x : values) {
				String line = x.toString();
				
				System.out.println("test: " + x);
				System.out.println("test: " + line);
				
				String[] row = line.split(",");

				// sum up temperature and humidity
				allTemp = allTemp + Double.parseDouble(row[1]);
				allHumidity = allHumidity + Double.parseDouble(row[2]);
				count++;

				// write output
				context.write(new Text(row[0]), new Text(row[1] + "," + row[2]));
			}

			double avgTemp = allTemp / count;
			double avgHumidity = allHumidity / count;
			String strValue = avgTemp + "," + avgHumidity;

			// replace values in newCentroids
			Configuration conf = context.getConfiguration();
			conf.unset("newcentroid:" + key.toString());
			conf.set("newcentroid:" + key.toString(), strValue);
			
		}
	}

	// Finalise MapReduce Function
	public static class FinaliseMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input key = station, values = temperature, humidity
			// output key = index, values = station,temperature,humidity

			// split values to temperature and humidity
			String stationValues[] = values.toString().split(",");

			// initialise variable
			double curDistance = 0;

			// initialise for index 0
			Configuration conf = context.getConfiguration();

			String[] centroidOne = conf.get("oldcentroid:0").split(",");

			double tempDistance = Math.abs(Double.parseDouble(centroidOne[0]) - Double.parseDouble(stationValues[0]));
			double humidityDistance = Math
					.abs(Double.parseDouble(centroidOne[1]) - Double.parseDouble(stationValues[1]));

			double minDistance = tempDistance + humidityDistance;

			int kmeansIndex = 0;

			// for loop to check assign clusters
			for (int i = 1; i < k; i++) {
				String centroidValues[] = conf.get("oldcentroid:" + i).split(",");
				tempDistance = Math.abs(Double.parseDouble(centroidValues[0]) - Double.parseDouble(stationValues[0]));
				humidityDistance = Math
						.abs(Double.parseDouble(centroidValues[1]) - Double.parseDouble(stationValues[1]));
				curDistance = tempDistance + humidityDistance;

				if (curDistance < minDistance) {
					kmeansIndex = i;
					minDistance = curDistance;
				}

			}
			String strValues = key.toString() + "," + stationValues[0] + "," + stationValues[1];
			context.write(new Text(Integer.toString(kmeansIndex)), new Text(strValues));
		}
	}

	public static class FinaliseReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input key = index, iterable values = station,temperature,humidity
			// output key = index = station
			for (Text x : values) {
				String line = x.toString();
				String[] row = line.split(",");

				context.write(key, new Text(row[0]));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int iteration = 0;

		Configuration conf = new Configuration();

		// First MapReduce Driver Code
		Job job = Job.getInstance(conf);
		job.setJarByClass(WeatherMapReduce.class);
		job.setJobName("PreprocessAvgPerYear");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/iteration_" + iteration));
		job.setMapperClass(FirstMap.class);
		job.setCombinerClass(FirstReduce.class);
		job.setReducerClass(FirstReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		iteration++;

		// Second MapReduce Driver Code
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(WeatherMapReduce.class);
		job2.setJobName("PreprocessAvgAll");
		FileInputFormat.addInputPath(job2, new Path(args[1] + "/iteration_" + (iteration - 1)));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/iteration_" + iteration));
		job2.setMapperClass(SecondMap.class);
		job2.setCombinerClass(SecondReduce.class);
		job2.setReducerClass(SecondReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.waitForCompletion(true);
		iteration++;

		// K-means MapReduce Driver Code, loop using while condition, check using
		// if-else statement
		// initialisation
		conf.set("oldcentroid:" + 0, "17" + "," + "49");
		conf.set("oldcentroid:" + 1, "14" + "," + "73");
		conf.set("oldcentroid:" + 2, "-2" + "," + "77");
		conf.set("oldcentroid:" + 3, "27" + "," + "76");
		conf.set("newcentroid:" + 1, "17" + "," + "49");
		conf.set("newcentroid:" + 2, "14" + "," + "73");
		conf.set("newcentroid:" + 3, "-2" + "," + "77");
		conf.set("newcentroid:" + 4, "27" + "," + "76");

		boolean centroidsConverge = false;
		
		while (!centroidsConverge) {
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(WeatherMapReduce.class);
			job3.setMapperClass(KMeansMap.class);
			job3.setCombinerClass(KMeansReduce.class);
			job3.setReducerClass(KMeansReduce.class);
			job3.setInputFormatClass(KeyValueTextInputFormat.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job3, new Path(args[1] + "/iteration_" + (iteration - 1)));
			FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/iteration_" + iteration));
			job3.waitForCompletion(true);

			// assume is true first
			centroidsConverge = true;

			for (int i = 0; i < k; i++) {
				boolean checkTemp = false;
				boolean checkHumidity = false;
				String[] oldSplit = conf.get("oldcentroid:" + "i").split(",");
				String[] newSplit = conf.get("newcentroid:" + "i").split(",");

				checkTemp = (Double.parseDouble(oldSplit[0]) - Double.parseDouble(newSplit[0])) <= threshold;
				checkHumidity = (Double.parseDouble(oldSplit[1]) - Double.parseDouble(newSplit[1])) <= threshold;

				if (!checkTemp || !checkHumidity) {
					// if check fails, set back to false to continue iteration
					centroidsConverge = false;
				}
			}
			// increase iteration value
			iteration++;

			if (!centroidsConverge) {
				for (int i = 0; i < k; i++) {
					conf.unset("oldcentroid:" + i);
					conf.set("oldcentroid:" + i, conf.get("newcentroid:" + i));
				}
			}
		}

		// Finalise MapReduce Driver Code, format according to the required format
		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(WeatherMapReduce.class);
		job4.setJobName("FinaliseOutput");
		FileInputFormat.addInputPath(job4, new Path(args[1] + "/iteration_" + (iteration - 1)));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/iteration_" + iteration));
		job4.setMapperClass(FinaliseMap.class);
		job4.setCombinerClass(FinaliseReduce.class);
		job4.setReducerClass(FinaliseReduce.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.waitForCompletion(true);
		
	}

}
