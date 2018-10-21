package com.ecommerce.log;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AddtoCartMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		IntWritable one = new IntWritable(1);
		String ILine = value.toString();
		
		final String tokens[] = StringUtils.splitPreserveAllTokens(ILine,",");
		if(StringUtils.equals(tokens[2],"AddtoCart")||StringUtils.equals(tokens[2], "Purchase"))
		{
			context.write(new Text(tokens[0]+"_"+tokens[1]),one);
		}
	}


}
