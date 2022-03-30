package join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class SocialTriangle_RS extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(SocialTriangle_RS.class);
	//private static final String INTERMEDIATE_PATH = "intermediate_output";
	private static final int MAX = 50000;

	enum TriangleCount { 		// Global Counter
		COUNT
	}

	// Job1 Mapper
	public static class Path2Mapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			
			String[] userIds = value.toString().split(",");
			String userX = userIds[0];
			String userY = userIds[1];

			
			if (Integer.parseInt(userX) < MAX && Integer.parseInt(userY) < MAX) {
				Text outKey = new Text();
				Text outValue = new Text();

				outKey.set(userX);
				outValue.set("O" + userY);

				
				// Emit X and Y as X follows Y by putting a flag "O"
				// X->Y
				context.write(outKey, outValue);

				// Emit y is being followed by X by puutting a flag as "I"
				//Y<-X
				outKey.set(userY);
				outValue.set("I" + userX);

				context.write(outKey, outValue);
				
			}
		}
	}

	// Job1 Reducer
	public static class Path2Reducer extends Reducer<Text, Text, NullWritable, Text> {
		private List<Text> listIncoming = new ArrayList<>();
		private List<Text> listOutgoing = new ArrayList<>();

		
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			listIncoming.clear();
			listOutgoing.clear();

			// Separte the values on basis of flags assigned
			for(Text t: values) {
				if(t.charAt(0) == 'O'){
					listOutgoing.add(new Text(t.toString().substring(1)));
				} else if(t.charAt(0) == 'I') {
					listIncoming.add(new Text(t.toString().substring(1)));
				}
			}

			// For each Z and each X emit (X,Y,Z) where X follows Y and Y follows Z
			// and Z and X cannot be equal
			for(Text Z: listOutgoing) {
				if(Z.getLength() > 0) {
					for(Text X: listIncoming) {
						if(X.getLength() > 0 && !X.toString().equals(Z.toString())) {
							context.write(NullWritable.get(), new Text(X+","+key.toString()+","+Z));
						}
					}
				}

			}

		}
	}

	// Job2 Mapper 1
	public static class Path2ModMapper extends Mapper<Object, Text, Text, Text> {

       // Input for this mapper is output of Path2Reducer ie X,Y,Z
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			String[] mappedUserIds = value.toString().split(",");

			int userX = Integer.parseInt(mappedUserIds[0]);
			int userZ = Integer.parseInt(mappedUserIds[2]);

			Text Key2 = new Text();
			Text value2 = new Text();
			//Emitting the combination X,Z to check if the connection exists
			Key2.set(userX+","+userZ);
			// Setting flag "M" that means this input is mapped once 
			value2.set("M");

			// Since, the join is between X follows Z and Z follow X for triangle,
			// the output for mapper 1 is ((X,Z), "M")
			context.write(Key2,value2);
		}
	}

	// Job2 Mapper 2
	public static class EdgesMapper extends Mapper<Object, Text, Text, Text> {

		// The input to mapper is edges.csv
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			String userIds[] = value.toString().split(",");
			String userZ = userIds[0];
			String userX = userIds[1];

			if (Integer.parseInt(userX) < MAX && Integer.parseInt(userZ) < MAX) {
				int X = Integer.parseInt(userX);
				int Z = Integer.parseInt(userZ);

				Text Key2 = new Text();
				Text value2 = new Text();
				// Joining using X,Z,  and emitting as key
				Key2.set(userX+","+userZ);
				// Setting flag "N" that means this input is new and from the datafile edges.csv

				value2.set("N");

				// the output for mapper 2 is ((X,Z), "N")
				context.write(Key2,value2);
			}
		}
	}

	// Job2 Reducer
	public static class ClosedTriangle extends Reducer<Text, Text, NullWritable, Text> {
		int sum = 0;

		// The input to reducer is ((X,Z),["E","Y",....])
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			int edgesVal_m = 0;
			int pathVal_n = 0;

			// Separating both the values from mapper based on "E" and "Y"
			for(Text t: values) {
				if(t.charAt(0) == 'M') {
					pathVal_n++;
				} else if (t.charAt(0) == 'N'){
					edgesVal_m++;
				}
			}

			// The total number of triangles will be Total number of Y's multiplied by total E's
			// X->[Y...]->Z and X->[E....]->Z
			sum += pathVal_n*edgesVal_m;
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			// add the sum from each map task to the global counter
			context.getCounter(TriangleCount.COUNT).increment(sum);
		}
	}




	@Override
	public int run(final String[] args) throws Exception {
		final Configuration conf = getConf();
		conf.set("mapred.child.java.opts", "-Xmx8192m");
		final Job job = Job.getInstance(conf, "Path length 2");
		job.setJarByClass(SocialTriangle_RS.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

		job.setMapperClass(Path2Mapper.class);
		job.setReducerClass(Path2Reducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// The input to the first job is the edges.csv file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		// first job writes the output to a intermediate file
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		final Job job2 = Job.getInstance(conf, "Join job");
		job2.setJarByClass(SocialTriangle_RS.class);
		final Configuration jobConf2 = job2.getConfiguration();
		jobConf2.set("mapreduce.output.textoutputformat.separator", "\t");

		// The second job has two mappers as input.
		// the first mapper has input i.e the output of the job 1.
		// The second mapper has input as the edges.csv file
		MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, Path2ModMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, EdgesMapper.class);

		job2.setReducerClass(ClosedTriangle.class);

		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		// The output of the job2 is written to the output directory
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		int r = job2.waitForCompletion(true) ? 0 : 1;

		// The output is inside the global counter. since each triangle is counted thrice
		// the output is divided by 3.
		
		Counter counter = job2.getCounters().findCounter(TriangleCount.COUNT);
		System.out.println(counter.getDisplayName() + ":" +counter.getValue()/3);

		return r;
	}


	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir> <output1-dir>");
		}

		try {
			ToolRunner.run(new SocialTriangle_RS(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}

