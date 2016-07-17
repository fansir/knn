package knn.mr;

import java.io.IOException;

import knn.kvtype.TestRecord;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author 许国杰
 * @date 2016-7-15
 * */
public class KnnReducer extends Reducer<IntWritable, TestRecord, NullWritable, Text> {

	private Text value = new Text();
	
	@Override
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
		//计算预测值,写入Output
		StringBuffer valueBuffer = new StringBuffer();
		for(int i=0;i<resultTestRecord.getAttributes().length;i++)
			valueBuffer.append(resultTestRecord.getAttributes()[i]).append(",");
		valueBuffer.append(resultTestRecord.getMostType());
		
		value.set(valueBuffer.toString());
		cxt.write(NullWritable.get(),value);
	}
	
}
