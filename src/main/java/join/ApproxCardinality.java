package join;


import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class ApproxCardinality extends Configured implements Tool {
	private static final int MAX = 7812500;
	enum Cardinality {
		COUNT
	}
	private static final Logger logger = LogManager.getLogger(ApproxCardinality.class);

	public static class Path2Mapper extends Mapper<Object, Text, Text, Text> {
		private Text m = new Text();
		private Text follower_m = new Text();

		private Text n = new Text();
		private Text follower_n = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] userIds = value.toString().split(",");
			String userX = userIds[0];
			String userY = userIds[1];
			
 
			// Adding Max filter
			
			if (Integer.parseInt(userX) < MAX && Integer.parseInt(userY) < MAX) {

				m.set(userX);
				// for setting X follows Y by adding flag "O" which means outgoing
				follower_m.set("O" + userY);
				context.write(m,follower_m);
	
				n.set(userY);
				follower_n.set("I" + userX);
				
				// for setting Y followed by X by adding flag "I" which means incoming
				context.write(n,follower_n);
			}		
			
		}
	}

	public static class Path2Reducer extends Reducer<Text, Text, Text, LongWritable> {
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			long incomingSum = 0;
			long outgoingSum = 0;
			
			// Separate edges whether incoming or outgoing
			for (Text t : values) {
				//System.out.println("start");
				//System.out.println(t.charAt(0));
				if (t.charAt(0) == 'I') {
					//System.out.println("if");
					//System.out.println(incomingSum);
					incomingSum += 1;
				} else if (t.charAt(0) == 'O') {
					//System.out.println("else");
					//System.out.println(outgoingSum);
					outgoingSum += 1;
				}
			}

			// incomingSum represents m and outgoingSum represents n
            // m*n
			long total_count = incomingSum * outgoingSum;


			// adding the count to the global counter
			context.getCounter(Cardinality.COUNT).increment(total_count);
			LongWritable paths = new LongWritable(total_count);

			context.write(key,paths);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "ApproxCardinality");
		job.setJarByClass(ApproxCardinality.class);

		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		conf.set("mapred.child.java.opts", "-Xmx8192m");

		// Delete output directory, only to ease local development; will not work on AWS. ===========
//		final FileSystem fileSystem = FileSystem.get(conf);
//		if (fileSystem.exists(new Path(args[1]))) {
//			fileSystem.delete(new Path(args[1]), true);
//		}
		// ================
		job.setMapperClass(Path2Mapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Path2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		int jobCompleted =  job.waitForCompletion(true) ? 0 : 1;
		Counter counter = job.getCounters().findCounter(Cardinality.COUNT);

		
		System.out.println(counter.getDisplayName() + ":" +counter.getValue());

		return jobCompleted;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new ApproxCardinality(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

