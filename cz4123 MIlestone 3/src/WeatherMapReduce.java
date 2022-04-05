
// import necessary libraries
import java.lang.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringJoiner;
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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WeatherMapReduce {

	// initialisation of variables
	// threshold to check to run another iteration of kmeans, k to indicate no of centroids
	public final static double threshold = 0.001;
	public final static int k = 4;
	
	// Read centroids from HDFS based on job 3 iteration
	public static String[] readCentroids(Configuration conf, String pathString) throws IOException, FileNotFoundException{
		
		//https://stackoverflow.com/questions/14573209/read-a-text-file-from-hdfs-line-by-line-in-mapper
		
		// create array variable to store centroids retrieved from job 3 output
		String[] newCentroids = new String[k];
		
		// open file location 
		Path path = new Path(pathString);
		FileSystem hdfs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));
		
		// read file
		try {
			String line;
			line = br.readLine();
			
			while (line != null) {
				// split the line to key and value pair
				String[] keyValueSplit = line.split("\t");
				int centroidID = Integer.parseInt(keyValueSplit[0]);
				// split the values to temperature and humidity 
				String[] values = keyValueSplit[1].split(",");
				
				System.out.println("key: " + keyValueSplit[0]);
				System.out.println("values: " + keyValueSplit[1]);
				
				// assign the value to the array variable
				newCentroids[centroidID] = values[0] + "," + values[1];
				
				// read the next line to prevent infinite loops
				line = br.readLine(); 
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
			
		}
		// return result
		return newCentroids;
	}
	
	// First Map Function - to get average temperature and humidity based on 1 year
	public static class FirstMap extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			// input: key = index | values = station, timestamp, temperature, humidity
			// output: key = station, year | values = temperature, humidity
			
			// Create format for date time
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");

			// read csv files
			String line = values.toString();
			// split the line
			String[] row = line.split(",");
			// assign key - station,year
			
			// M equals null value, ensure only non null value is passed
			if (!row[2].equals("M") && !row[3].equals("M")) {
				// format the timestamp
				LocalDateTime datetime = LocalDateTime.parse(row[1], formatter);
				// pass key  as key = station, year
				String strKey = row[0] + "," + datetime.getYear();
				// pass values as values = temperature and humidity
				String strValue = row[2] + "," + row[3];
				// write map output
				context.write(new Text(strKey), new Text(strValue));
			}
		}
	}

	// First Reduce Function - to get average temperature and humidity based on 1 year
	public static class FirstReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input: key = station, year | values = iterable(temperature, humidity)
			// output: key = station, year | values = avgtemperature, avghumidity
			// value is list inside list
			
			// initialisation variables
			int count = 0;
			double temp = 0;
			double humidity = 0;

			// iterate through iterable
			for (Text x : values) {
				// [temperature, humidity]
				String line = x.toString();
				String[] row = line.split(",");
				count++;
				temp = temp + Double.parseDouble(row[0]);
				humidity = humidity + Double.parseDouble(row[1]);
			}
			
			// compute the average temperature and humidity + format to string
			double avgTemp = temp / count;
			double avgHumidity = humidity / count;
			String strValue = avgTemp + "," + avgHumidity;
			
			// write reduce output
			context.write(key, new Text(strValue));
		}
	}

	// Second Map Function - to get average temperature and humidity based on all the years
	public static class SecondMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input: key = station, year | values = avgtemperature, avghumidity
			// output: key = station | values = avgtemperature, avghumidity
			// assume key and value is set nicely
			
			// remove the year from key
			String line = key.toString();
			String[] row = line.split(",");
			String newStrKey = row[0];
			
			// write map output
			context.write(new Text(newStrKey), values);
		}
	}

	// Reduce Function - to get average temperature and humidity based on all the years
	public static class SecondReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input: key = station, year | values = iterable(avgtemperature, avghumidity)
			// output: key = station | values = avgtemperature, avghumidity
			
			// initialise variables
			int count = 0;
			double temp = 0;
			double humidity = 0;
			
			// iterate through iterable to sum up temperature, humidity, count
			for (Text x : values) {
				String line = x.toString();
				String[] row = line.split(",");
				count++;
				temp = temp + Double.parseDouble(row[0]);
				humidity = humidity + Double.parseDouble(row[1]);
			}
			// compute the average temperature and humidity + format to string
			double avgTemp = temp / count;
			double avgHumidity = humidity / count;
			String strValue = avgTemp + "," + avgHumidity;
			
			// write reduce output
			context.write(key, new Text(strValue));

		}
	}

	// Kmeans Map Function - assign clusters
	public static class KMeansMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input key = station, values = temperature, humidity
			// output key = centroidID, values = station,temperature,humidity

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

			// for loop to check and assign clusters
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
			
			// format values
			String strValues = key.toString() + "," + stationValues[0] + "," + stationValues[1];
			
			// write map output
			context.write(new Text(Integer.toString(kmeansIndex)), new Text(strValues));
		}
	}

	// Kmeans Reduce Function - recompute centroids values
	public static class KMeansReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input key = centroidID, iterable values = station,temperature,humidity
			// output key = centroidID, values = temperature, humidity
			
			// initialise variable
			double allTemp = 0;
			double allHumidity = 0;
			int count = 0;
			
			// iterate through iterable 
			for (Text x : values) {
				String line = x.toString();
				String[] row = line.split(",");
				
				// sum up temperature and humidity
				allTemp = allTemp + Double.parseDouble(row[1]);
				allHumidity = allHumidity + Double.parseDouble(row[2]);
				count++;
			}
			
			// recompute the centroid cluster
			double avgTemp = allTemp / count;
			double avgHumidity = allHumidity / count;
			String strValues = avgTemp + "," + avgHumidity;
			
			// write reduce output
			context.write(key, new Text(strValues));
		}
	}

	// Finalise Map Function - assign clusters
	public static class FinaliseMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input key = station, values = temperature, humidity
			// output key = centroidID, values = station,temperature,humidity

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
			// write map output
			context.write(new Text(Integer.toString(kmeansIndex)), new Text(strValues));
		}
	}
	
	// Finalise Reduce function - output stations based on assigned clusters
	public static class FinaliseReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input key = centroidID, iterable values = station,temperature,humidity
			// output key = centroidID = list of stations
			
			StringJoiner joiner = new StringJoiner(",");
			
			// iterate through iterable
			for (Text x : values) {
				String line = x.toString();
				String[] row = line.split(",");
				
				joiner.add(row[0]);
			}
			String result = joiner.toString();
			// write reduce output
			context.write(key, new Text(result));
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
		// initialisation of centroid

		conf.set("oldcentroid:" + 0, "0" + "," + "0");
		conf.set("oldcentroid:" + 1, "10" + "," + "10");
		conf.set("oldcentroid:" + 2, "20" + "," + "20");
		conf.set("oldcentroid:" + 3, "30" + "," + "30");
		
		// initialise variable
		boolean centroidsConverge = false;
		
		// loop till centroids converge
		while (!centroidsConverge) {
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(WeatherMapReduce.class);
			job3.setMapperClass(KMeansMap.class);
			//job3.setCombinerClass(KMeansReduce.class); --> comment it will prevent station from being remove from value ??
			job3.setReducerClass(KMeansReduce.class);
			job3.setInputFormatClass(KeyValueTextInputFormat.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job3, new Path(args[1] + "/iteration_" + 1));
			FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/iteration_" + iteration));
			job3.waitForCompletion(true);

			// assume is true first
			centroidsConverge = true;
			
			// retrieve new centroids values from reduce output
			String[] newCentroids = readCentroids(conf, args[1] + "/iteration_" + iteration + "/part-r-00000");
			
			// for loop to ensure all centroids values converge
			for (int i = 0; i < k; i++) {
				boolean checkTemp = false;
				boolean checkHumidity = false;
				String[] oldSplit = conf.get("oldcentroid:" + i).split(",");
				String[] newSplit = newCentroids[i].split(",");
				
				System.out.println("old temperature" + oldSplit[0]);
				System.out.println("new temperature" + newSplit[0]);
				
				checkTemp = (Double.parseDouble(oldSplit[0]) - Double.parseDouble(newSplit[0])) <= threshold;
				checkHumidity = (Double.parseDouble(oldSplit[1]) - Double.parseDouble(newSplit[1])) <= threshold;

				if (!checkTemp || !checkHumidity) {
					// if check fails, set back to false to continue iteration
					centroidsConverge = false;
				}
			}
			// increase iteration value
			iteration++;
			
			// assign new centroids value to old centroids values if convergence fails 
			if (!centroidsConverge) {
				for (int i = 0; i < k; i++) {
					conf.unset("oldcentroid:" + i);
					conf.set("oldcentroid:" + i, newCentroids[i]);
				}
			}
		}

		// Finalise MapReduce Driver Code, format according to the required format
		Job job4 = Job.getInstance(conf);
		job4.setJarByClass(WeatherMapReduce.class);
		job4.setJobName("FinaliseOutput");
		FileInputFormat.addInputPath(job4, new Path(args[1] + "/iteration_" + 1));
		FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/iteration_" + iteration));
		job4.setMapperClass(FinaliseMap.class);
		//job4.setCombinerClass(FinaliseReduce.class);
		job4.setReducerClass(FinaliseReduce.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.waitForCompletion(true);
		
	}

}
