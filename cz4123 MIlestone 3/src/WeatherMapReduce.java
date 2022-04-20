
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.StringJoiner;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class WeatherMapReduce {

	// initialisation of variables
	// threshold to check to run another iteration of kmeans, k to indicate no of
	// centroids
	public final static double threshold = 0.001;
	public static int k = 4;
	public static int dataSize = 32;

	// Read centroids from HDFS based on job 3 iteration
	public static String[] readCentroids(Configuration conf, String pathString)
			throws IOException, FileNotFoundException {

		// https://stackoverflow.com/questions/14573209/read-a-text-file-from-hdfs-line-by-line-in-mapper

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

				// assign the value to the array variable
				newCentroids[centroidID] = keyValueSplit[1];

				// read the next line to prevent infinite loops
				line = br.readLine();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();

		}
		// return result
		return newCentroids;
	}

	public static String[] centroidsInit(Configuration conf, String pathString)
			throws IOException, FileNotFoundException {

		// initialise
		String[] oldCentroids = new String[k];
		ArrayList<Integer> positions = new ArrayList<Integer>();

		Random random = new Random();

		int pos;

		while (positions.size() < k) {
			pos = random.nextInt(dataSize);
			// check if position already exists
			if (!positions.contains(pos)) {
				positions.add(pos);

			}
		}
		// sort the positions
		Collections.sort(positions);

		// open file location
		Path path = new Path(pathString);
		FileSystem hdfs = FileSystem.get(conf);
		BufferedReader br = new BufferedReader(new InputStreamReader(hdfs.open(path)));

		// read file
		try {
			String line;
			line = br.readLine();
			int fileindexpos = 0;
			int arrayindexpos = 0;
			while (line != null && arrayindexpos < k) {

				int curpos = positions.get(arrayindexpos);
				if (curpos == fileindexpos) {
					// split the line to key and value pair
					// key is station, values is temperature,humidity
					String[] keyValueSplit = line.split("\t");
					oldCentroids[arrayindexpos] = keyValueSplit[1];
					arrayindexpos++;
				}

				// read the next line to prevent infinite loops
				fileindexpos++;
				line = br.readLine();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			br.close();
		}

		return oldCentroids;
	}

	public static String[] centroidsInit2() {
		String[] oldCentroids = new String[k];

		oldCentroids[0] = "25.1006791;26.35799778;27.44876002;27.9251813;27.25231388;26.06718851;25.58552712;25.80387655;26.63731523;"
				+ "27.11733535;26.72576198;25.67240075,76.73102315;75.21675327;75.53543488;73.75664688;73.05795495;76.32875389;77.25904829;"
				+ "76.39262479;75.85016531;75.62142427;72.74661157;75.09638404";

		oldCentroids[1] = "17.17580105;19.23602543;20.74712271;22.00578838;22.79875658;23.96050118;23.49556867;23.57226129;23.35334141;"
				+ "22.54666405;19.77682044;17.52889883,44.04263278;39.35707501;37.01085605;31.11067463;33.50676538;43.51047015;50.5632079;"
				+ "51.38787451;51.22445073;45.30493944;43.40988249;43.85481327";

		oldCentroids[2] = "-11.37791125;-8.88550606;-2.46782327;5.83245943;12.76368631;17.57740719;19.51253796;17.65210581;11.75446767;"
				+ "4.41493321;-4.63420549;-8.96597388,78.4160057;75.74876254;68.87341752;59.58013801;59.23489847;64.41462075;70.61054862;"
				+ "72.43022352;74.32342214;73.8321926;79.32049237;81.06695815";

		oldCentroids[3] = "6.79643096;8.86890806;12.93279208;17.14892561;21.46365705;24.37250225;26.51255365;26.78874031;23.15902877;"
				+ "17.9468298;13.05389539;8.12296815,74.91861627;71.89637188;71.10172344;71.20357909;72.70996632;78.43492434;78.73857562;"
				+ "77.65831353;78.56124365;78.19998888;76.53555707;75.24748671";

		return oldCentroids;
	}

	// First Map Function - to get average temperature and humidity based on 1 year
	public static class FirstMap extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			// input: key = index | values = station, timestamp, temperature, humidity
			// output: key = station, month | values = temperature, humidity

			// Create format for date time
			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
//			DateTimeFormatter formatter = DateTimeFormatter.ofPattern("d/M/yyyy H:mm");

			// read csv files
			String line = values.toString();
			// split the line
			String[] row = line.split(",");
			// assign key - station,year

			// M equals null value, ensure only non null value is passed
			if (!row[2].equals("M") && !row[3].equals("M")) {
				// format the timestamp
				LocalDateTime datetime = LocalDateTime.parse(row[1], formatter);
				// pass key as key = station, year
				String strKey = row[0] + "," + datetime.getMonthValue();
				// pass values as values = temperature and humidity
				String strValue = row[2] + "," + row[3];
				// write map output
				context.write(new Text(strKey), new Text(strValue));
			}

		}
	}

	// First Reduce Function - to get average temperature and humidity based on 1
	// year
	public static class FirstReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input: key = station, year | values = iterable(temperature, humidity)
			// output: key = station, year | values = avgtemperature, avghumidity
			// value is list inside list

			// initialisation variables
			int count = 0;
			double temp = 0;
			double humidity = 0;
			String strValue = null;
			// for min, max, median function
			ArrayList<Double> tempList = new ArrayList<Double>();
			ArrayList<Double> humidityList = new ArrayList<Double>();

			// get configuration
			Configuration conf = context.getConfiguration();
			String method = conf.get("method");

			if (method.equals("median")) {
				// iterate through iterable
				for (Text x : values) {
					// [temperature, humidity]
					String line = x.toString();
					String[] row = line.split(",");
					tempList.add(Double.parseDouble(row[0]));
					humidityList.add(Double.parseDouble(row[1]));
				}

				// sort the list
				Collections.sort(tempList);
				Collections.sort(humidityList);

				// assign median value
				// http://www.java2s.com/example/java-utility-method/median/median-arraylist-double-values-82543.html
				if (tempList.size() % 2 == 1) {
					temp = tempList.get((tempList.size() + 1) / 2 - 1);
					humidity = humidityList.get((humidityList.size() + 1) / 2 - 1);
				} else {
					// temperature
					double lowerTemp = tempList.get(tempList.size() / 2 - 1);
					double upperTemp = tempList.get(tempList.size() / 2);

					temp = (lowerTemp + upperTemp) / 2.0;

					// humidity
					double lowerHumidity = humidityList.get(humidityList.size() / 2 - 1);
					double upperHumidity = humidityList.get(humidityList.size() / 2);

					humidity = (lowerHumidity + upperHumidity) / 2.0;
				}
				strValue = temp + "," + humidity;
			} else if (method.equals("min")) {
				// iterate through iterable
				for (Text x : values) {
					// [temperature, humidity]
					String line = x.toString();
					String[] row = line.split(",");
					tempList.add(Double.parseDouble(row[0]));
					humidityList.add(Double.parseDouble(row[1]));
				}

				// sort the list
				Collections.sort(tempList);
				Collections.sort(humidityList);

				// assign min
				temp = tempList.get(0);
				humidity = humidityList.get(0);

				strValue = temp + "," + humidity;
			} else if (method.equals("max")) {
				// iterate through iterable
				for (Text x : values) {
					// [temperature, humidity]
					String line = x.toString();
					String[] row = line.split(",");
					tempList.add(Double.parseDouble(row[0]));
					humidityList.add(Double.parseDouble(row[1]));
				}

				// sort the list
				Collections.sort(tempList);
				Collections.sort(humidityList);

				// find the size
				int n = tempList.size();

				// assign max
				temp = tempList.get(n - 1);
				humidity = humidityList.get(n - 1);

				strValue = temp + "," + humidity;
			}
			// default or mean
			else {
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
				strValue = avgTemp + "," + avgHumidity;
			}

			// write reduce output
			context.write(key, new Text(strValue));
		}
	}

	// Second Map Function - to get average temperature and humidity based on all
	// the years
	public static class SecondMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input: key = station, month | values = avgtemperature, avghumidity
			// output: key = station | values = month, avgtemperature, avghumidity
			// assume key and value is set nicely

			// remove the year from key
			String line = key.toString();
			String[] row = line.split(",");
			String newStrKey = row[0];
			String newStrValues = row[1].toString() + "," + values;

			// write map output
			context.write(new Text(newStrKey), new Text(newStrValues));
		}
	}

	// Reduce Function - to get average temperature and humidity based on all the
	// years
	public static class SecondReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input: key = station | values = iterable(month, avgtemperature, avghumidity)
			// output: key = station | values = avgtemperature x12 , avghumidity x12 [temp
			// or humidity seperated by ;]

			// initialisation
			String[] tempArr = new String[12];
			String[] humidityArr = new String[12];

			for (Text x : values) {
				String line = x.toString();
				String[] row = line.split(",");
				tempArr[Integer.parseInt(row[0]) - 1] = row[1];
				humidityArr[Integer.parseInt(row[0]) - 1] = row[2];
			}

			StringJoiner joiner = new StringJoiner(";");
			StringJoiner joiner2 = new StringJoiner(";");

			for (int i = 0; i < 12; i++) {
				joiner.add(tempArr[i]);
				joiner2.add(humidityArr[i]);
			}

			// store results as a lists of string
			String strValues = joiner.toString() + "," + joiner2.toString();

			// write reduce output
			context.write(key, new Text(strValues));
		}
	}

	// Kmeans Map Function - assign clusters
	public static class KMeansMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input key = station, values = temperature x12, humidity x12
			// output key = centroidID, values = temperature,humidity

			// split values to temperature and humidity
			String stationValues[] = values.toString().split(",");
			// split values to months for temperature and humidity
			String tempArr[] = stationValues[0].split(";");
			String humidityArr[] = stationValues[1].split(";");

			// initialise variable
			double curDistance = 0;
			double tempDistance = 0;
			double humidityDistance = 0;
			double minDistance = 0;

			// initialise for index 0
			Configuration conf = context.getConfiguration();

			String[] centroidOne = conf.get("oldcentroid:0").split(",");
			String[] centroidOneTempArr = centroidOne[0].split(";");
			String[] centroidOneHumidityArr = centroidOne[1].split(";");

			for (int i = 0; i < 12; i++) {
				tempDistance = tempDistance
						+ (Math.abs(Double.parseDouble(centroidOneTempArr[i]) - Double.parseDouble(tempArr[i])));
				humidityDistance = humidityDistance + (Math
						.abs(Double.parseDouble(centroidOneHumidityArr[i]) - Double.parseDouble(humidityArr[i])));
			}

			minDistance = tempDistance + humidityDistance;

			int kmeansIndex = 0;

			// for loop to check and assign clusters
			for (int i = 1; i < k; i++) {

				String[] centroidValues = conf.get("oldcentroid:" + i).split(",");
				String[] centroidTempArr = centroidValues[0].split(";");
				String[] centroidHumidityArr = centroidValues[1].split(";");

				// reset variables
				tempDistance = 0;
				humidityDistance = 0;
				curDistance = 0;

				for (int j = 0; j < 12; j++) {
					tempDistance = tempDistance
							+ (Math.abs(Double.parseDouble(centroidTempArr[j]) - Double.parseDouble(tempArr[j])));
					humidityDistance = humidityDistance + (Math
							.abs(Double.parseDouble(centroidHumidityArr[j]) - Double.parseDouble(humidityArr[j])));
				}

				curDistance = tempDistance + humidityDistance;

				if (curDistance < minDistance) {
					kmeansIndex = i;
					minDistance = curDistance;
				}
			}

			// write map output
			context.write(new Text(Integer.toString(kmeansIndex)), values);
		}
	}

	// Kmeans Reduce Function - recompute centroids values
	public static class KMeansReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input key = centroidID, iterable values = temperature, humidity in 24D
			// output key = centroidID, values = temperature, humidity in 24D

			// initialise variable
//			double allTemp = 0;
//			double allHumidity = 0;
//			int count = 0;

			int count = 0;
			Double allTempArr[] = new Double[12];
			Double allHumidityArr[] = new Double[12];

			// initialise arrayvalues to 0 to prevent null pointer exception
			for (int i = 0; i < 12; i++) {
				allTempArr[i] = 0.0;
				allHumidityArr[i] = 0.0;
			}

			// iterate through iterable
			for (Text x : values) {
				// convert to string
				String line = x.toString();
				// split into temperature and humidity
				String[] stationValues = line.split(",");
				String curTempArr[] = stationValues[0].split(";");
				String curHumidityArr[] = stationValues[1].split(";");

				// loop through the months
				for (int i = 0; i < 12; i++) {
					allTempArr[i] = allTempArr[i] + Double.parseDouble(curTempArr[i]);
					allHumidityArr[i] = allHumidityArr[i] + Double.parseDouble(curHumidityArr[i]);
				}
				count++;
			}

			// recompute the centroid cluster
			for (int j = 0; j < 12; j++) {
				allTempArr[j] = allTempArr[j] / count;
				allHumidityArr[j] = allHumidityArr[j] / count;
			}

			// format to string
			StringJoiner joiner = new StringJoiner(";");
			StringJoiner joiner2 = new StringJoiner(";");

			for (int k = 0; k < 12; k++) {
				joiner.add(allTempArr[k].toString());
				joiner2.add(allHumidityArr[k].toString());
			}

			// store results as a lists of string
			String strValues = joiner.toString() + "," + joiner2.toString();

			// write reduce output
			context.write(key, new Text(strValues));
		}
	}

	// Finalise Map Function - assign clusters
	public static class FinaliseMap extends Mapper<Text, Text, Text, Text> {
		public void map(Text key, Text values, Context context) throws IOException, InterruptedException {
			// input key = station, values = temperature, humidity
			// output key = centroidID, values = station

			// split values to temperature and humidity
			String stationValues[] = values.toString().split(",");
			// split values to months for temperature and humidity
			String tempArr[] = stationValues[0].split(";");
			String humidityArr[] = stationValues[1].split(";");

			// initialise variable
			double curDistance = 0;
			double tempDistance = 0;
			double humidityDistance = 0;
			double minDistance = 0;

			// initialise for index 0
			Configuration conf = context.getConfiguration();

			String[] centroidOne = conf.get("oldcentroid:0").split(",");
			String[] centroidOneTempArr = centroidOne[0].split(";");
			String[] centroidOneHumidityArr = centroidOne[1].split(";");

			for (int i = 0; i < 12; i++) {
				tempDistance = tempDistance
						+ (Math.abs(Double.parseDouble(centroidOneTempArr[i]) - Double.parseDouble(tempArr[i])));
				humidityDistance = humidityDistance + (Math
						.abs(Double.parseDouble(centroidOneHumidityArr[i]) - Double.parseDouble(humidityArr[i])));
			}

			minDistance = tempDistance + humidityDistance;

			int kmeansIndex = 0;

			// for loop to check and assign clusters
			for (int i = 1; i < k; i++) {

				String[] centroidValues = conf.get("oldcentroid:" + i).split(",");
				String[] centroidTempArr = centroidValues[0].split(";");
				String[] centroidHumidityArr = centroidValues[1].split(";");

				// reset variables
				tempDistance = 0;
				humidityDistance = 0;
				curDistance = 0;

				for (int j = 0; j < 12; j++) {
					tempDistance = tempDistance
							+ (Math.abs(Double.parseDouble(centroidTempArr[j]) - Double.parseDouble(tempArr[j])));
					humidityDistance = humidityDistance + (Math
							.abs(Double.parseDouble(centroidHumidityArr[j]) - Double.parseDouble(humidityArr[j])));
				}

				curDistance = tempDistance + humidityDistance;

				if (curDistance < minDistance) {
					kmeansIndex = i;
					minDistance = curDistance;
				}
			}

			String strValues = key.toString();
			// write map output
			context.write(new Text(Integer.toString(kmeansIndex)), new Text(strValues));
		}
	}

	// Finalise Reduce function - output stations based on assigned clusters
	public static class FinaliseReduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// input key = centroidID, iterable values = station
			// output key = centroidID = list of stations

			StringJoiner joiner = new StringJoiner(",");

			// iterate through iterable
			for (Text x : values) {
				String line = x.toString();
				joiner.add(line);
			}
			String result = joiner.toString();
			// write reduce output
			context.write(key, new Text(result));
		}
	}

	public static void main(String[] args) throws Exception {
		int iteration = 0;

		Configuration conf = new Configuration();

		// set values based on inputs
		// available methods: median, mean(avg), min, max
		// default: mean
		try {
			conf.set("method", args[2]);
		} catch (Exception e) {
//			e.printStackTrace();
			conf.set("method", "mean");
		}

		// available methods: preset, random
		// default is preset 
		String initMethod = null;

		try {
			initMethod = args[3];
		} catch (Exception e) {
			initMethod = "preset";
		}

		// First MapReduce Driver Code
		Job job = Job.getInstance(conf);
		job.setJarByClass(WeatherMapReduce.class);
		job.setJobName("PreprocessBasedonMonth");
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
		job2.setJobName("PreprocessCombineMonth");
		FileInputFormat.addInputPath(job2, new Path(args[1] + "/iteration_" + (iteration - 1)));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/iteration_" + iteration));
		job2.setMapperClass(SecondMap.class);
//		job2.setCombinerClass(SecondReduce.class);
		job2.setReducerClass(SecondReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.waitForCompletion(true);
		iteration++;

		// K-means MapReduce Driver Code, loop using while condition, check using
		// if-else statement

		// randomise centroid's values
		String[] oldCentroidsArray = new String[k];
		if (initMethod.equals("random")) {
			oldCentroidsArray = centroidsInit(conf, args[1] + "/iteration_" + 1 + "/part-r-00000");
		} else {
			oldCentroidsArray = centroidsInit2();
		}

		// set centroid's values
		for (int i = 0; i < k; i++) {
			conf.set("oldcentroid:" + i, oldCentroidsArray[i]);
		}

		// initialise variable
		boolean centroidsConverge = false;

		// loop till centroids converge
		while (!centroidsConverge) {
			Job job3 = Job.getInstance(conf);
			job3.setJarByClass(WeatherMapReduce.class);
			job3.setMapperClass(KMeansMap.class);
			job3.setCombinerClass(KMeansReduce.class);
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
				String[] oldSplitTempArr = oldSplit[0].split(";");
				String[] oldSplitHumidityArr = oldSplit[1].split(";");

				String[] newSplit = newCentroids[i].split(",");
				String[] newSplitTempArr = newSplit[0].split(";");
				String[] newSplitHumidityArr = newSplit[1].split(";");

				for (int j = 0; j < 12; j++) {
					checkTemp = (Math.abs(Double.parseDouble(oldSplitTempArr[j])
							- Double.parseDouble(newSplitTempArr[j]))) <= threshold;
					checkHumidity = (Math.abs(Double.parseDouble(oldSplitHumidityArr[j])
							- Double.parseDouble(newSplitHumidityArr[j]))) <= threshold;

					if (!checkTemp || !checkHumidity) {
						// if check fails, set back to false to continue iteration
						centroidsConverge = false;
					}
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
		job4.setCombinerClass(FinaliseReduce.class);
		job4.setReducerClass(FinaliseReduce.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(KeyValueTextInputFormat.class);
		job4.waitForCompletion(true);

	}

}
