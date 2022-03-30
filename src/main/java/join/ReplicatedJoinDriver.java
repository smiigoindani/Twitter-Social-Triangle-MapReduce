package join;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ReplicatedJoinDriver extends Configured implements Tool {

	private static final Logger logger = LogManager.getLogger(ReplicatedJoinDriver.class);
	private static final int MAX = 40000;
	//Global counter
	public enum Cardinality {
		finalCount
	}

	public static class TriangleCountMapper extends
			Mapper<Object, Text, Text, Text> {

		private HashMap<String, ArrayList<String>> userIdMap = new HashMap<>();
		
		

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {

			//Retrieve from local cache
			URI[] cacheFiles = context.getCacheFiles();

			if (cacheFiles != null && cacheFiles.length > 0) {

				//Read input to a bufferedReader
				BufferedReader reader = new BufferedReader((new FileReader("edges.csv")));

				String line;
				while ((line = reader.readLine()) != null) {
					String[] userIds = line.split(",", -1);
					String userX = userIds[0];
					String userY = userIds[1];

					//Removing userIds which exceed the filter value
					if ((Integer.parseInt(userX) <= MAX) && (Integer.parseInt(userY) <= MAX)) {

						// add a new value if not present
						if (userIdMap.containsKey(userX)) {

							ArrayList<String> followingList = userIdMap.get(userX);
							followingList.add(userY);

						} else {
							// append if the key is present
							ArrayList<String> list = new ArrayList<>();
							list.add(userY);
							userIdMap.put(userX, list);
						}
					}
                }

				reader.close();
			}
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {


			//Initialize local counter to 0
			int counter = 0;

			String userIds[] = value.toString().split(",");
			String userX = userIds[0];
			String userY= userIds[1];

				//if key exists, we know there is a path-2-length possible
				if (userIdMap.containsKey(userY)) {

					//check if start and end value of userid match
					for (String t: userIdMap.get(userY)) {
						if (userIdMap.containsKey(t)) {

							if (userIdMap.get(t).contains(userX)) {

								
								counter++;
							}

						}
					}
				}
			

			
			context.getCounter(Cardinality.finalCount).increment(counter);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {

		int val;
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Replicated Join");
		job.setJarByClass(ReplicatedJoinDriver.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "");

		//Add the input to cache file
		job.addCacheFile(new URI(args[0] + "/edges.csv"));

		//set the sequential flow of job
		job.setMapperClass(TriangleCountMapper.class);
		// Setting Reducer Task to 0
		job.setNumReduceTasks(0);

		
		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		val = job.waitForCompletion(true) ? 0 : 1;

	
		Counters counters = job.getCounters();
		Counter c1 = counters.findCounter(Cardinality.finalCount);

		logger.info("TOTAL COUNT: " + c1.getValue() / 3);
		
		return val;
	}


	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output1-dir>");
		}

		try {
			ToolRunner.run(new ReplicatedJoinDriver(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}