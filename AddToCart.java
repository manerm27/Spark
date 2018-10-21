package com.ecommerce.log;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AddToCart extends Configured implements Tool {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration(Boolean.TRUE);
		
		try {
			ToolRunner.run(conf, new AddToCart(), args);
			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}

	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
	Configuration conf = super.getConf();
	
	Job addtocart = Job.getInstance(conf, AddToCart.class.getName());
	
	addtocart.setJarByClass(AddToCart.class);
	 
	TextInputFormat.addInputPath(addtocart, new Path(args[0]));
	addtocart.setInputFormatClass(TextInputFormat.class);
	
	addtocart.setMapperClass(AddtoCartMapper.class);
	addtocart.setReducerClass(AddToCartReducer.class);
	
	addtocart.setNumReduceTasks(4);
	
	addtocart.setMapOutputKeyClass(Text.class);
	addtocart.setMapOutputValueClass(IntWritable.class);
	
	addtocart.setOutputKeyClass(Text.class);
	addtocart.setMapOutputValueClass(Text.class);
	addtocart.waitForCompletion(Boolean.TRUE);
	
		return 0;
	}

	

}
