# Spark




package driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import mapper.MoviesMapper;
import reducer.MoviesReducer;

public class MoviesDriver extends Configured implements Tool {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("java usage " + MoviesDriver.class.getName()
					+ "/Path/To/HDFS/input/File/  /Output/HDFS/Path/Directory/");
			return;
		}

		Configuration conf = new Configuration(Boolean.TRUE);

		try {
			int i = ToolRunner.run(conf, new MoviesDriver(), args);
			System.out.println("i---->" + i);
			if (i == 0) {
				System.out.println("Success!!");
			} else {
				System.out.println("Failure!!");
			}
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Failed in Stack Trace");
		}
	}

	public int run(String[] args) throws Exception {

		// Step -1 : Get the configuration
		
		Configuration conf = super.getConf();

		// Step -2 : Create a Job Instance

		Job movies = Job.getInstance(conf, MoviesDriver.class.getName());

		// Step -3 : Setting the classpath using the jar file 
		
		movies.setJarByClass(MoviesDriver.class);
		
		// Step -4 : Setting the Input
		
		TextInputFormat.addInputPath(movies, new Path(args[0]));
		movies.setInputFormatClass(TextInputFormat.class);
		
		// Step -5 : Setting the output
		
		TextOutputFormat.setOutputPath(movies, new Path(args[1]));
		movies.setOutputFormatClass(TextOutputFormat.class);
		
		// Step -6 : Setting the Mapper
		
		movies.setMapperClass(MoviesMapper.class);
		
		// Step -7 : Setting Reducer Class
		
		movies.setReducerClass(MoviesReducer.class);
		
		// Step InBetween : Set number of reduce task
		
		movies.setNumReduceTasks(4);
		
		// Step -8 : Setting the Mapper output key and Value 
		
		movies.setMapOutputKeyClass(IntWritable.class);
		movies.setMapOutputValueClass(IntWritable.class);
		
		//Step -9 : Set the Reducer Output Key and Value Class
		
		movies.setOutputKeyClass(IntWritable.class);
		movies.setOutputValueClass(IntWritable.class);
		
		
		
		movies.waitForCompletion(Boolean.TRUE);
		return 0;
	}

}
