package com.ecommerce.log;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AddToCartReducer extends Reducer<Text, IntWritable, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int sum = 0;
		String iLine = key.toString();
		final String tokens[] = StringUtils.splitPreserveAllTokens(iLine, "_");
		for(IntWritable intWritable : value)
		{
			sum = sum + intWritable.get();
		}
		
		if(sum==1)
		{
			context.write(new Text(tokens[0]),new Text(tokens[1]));
		}
		
	}
	
	

}
