package knn.mr;

import java.io.IOException;

import knn.kvtype.TestRecord;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
/**
 * @author begin
 * @date 2016-7-15
 */
public class KnnCombiner extends Reducer<IntWritable, TestRecord, IntWritable, TestRecord> {
	
	@Override// 更新k个紧邻值
	public void reduce(IntWritable key, Iterable<TestRecord> values,Context cxt) 
			throws IOException, InterruptedException{
		TestRecord resultTestRecord=null;
		for(TestRecord d:values){
			if(resultTestRecord==null)
				resultTestRecord = d;
			else{
				resultTestRecord.merge(d);
			}
		}
		cxt.write(key, resultTestRecord);
	}

}
