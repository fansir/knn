package knn.mr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;

import knn.kvtype.TestRecord;
import knn.kvtype.TrainRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;
/**
 * @author 许国杰
 * @date 2016-7-15
 * */
public class KnnMapper extends Mapper<LongWritable, Text, IntWritable, TestRecord> {
	
	private ArrayList<TestRecord> testRecords= new ArrayList<TestRecord>();
	private int K;
	
	@Override // 读取test数据
	public void setup(Context cxt) throws IOException{
		Configuration conf= cxt.getConfiguration();
		K = conf.getInt("K", -1);

		String testPath= conf.get("TestFilePath");
		Path testDataPath= new Path(testPath);
		FileSystem fs = testDataPath.getFileSystem(conf);
		readTestData(fs,testDataPath);
	}
	@Override
	public void map(LongWritable key,Text value,Context cxt) throws NumberFormatException, IOException, InterruptedException{
		String[] line= value.toString().split(",");
		// 遍历所有测试数据，计算每个训练数据和测试数据的距离
		for(TestRecord testRecord:testRecords){
			TrainRecord trainRecord = new TrainRecord(testRecord, line);
			testRecord.addTrainRecord(trainRecord);
		}
	}
	
	@Override
	public void cleanup(Context cxt) throws IOException,InterruptedException{
		IntWritable id = new IntWritable();
		// 遍历输出
		for(int i=0;i<testRecords.size();i++){
			id.set(i);
			cxt.write(id, testRecords.get(i));
		}
	}
	

	/**
	 * @param fs
	 * @param Path
	 * @throws IOException 
	 */
	private void readTestData(FileSystem fs,Path Path) throws IOException {
		LineReader in = new LineReader(fs.open(Path));
		Text line = new Text();
		while(in.readLine(line)>0){
			String[] testData= line.toString().split(",");
			float[] attribute= new float[testData.length];
			for(int i=0;i<testData.length;i++){
				attribute[i]=Float.parseFloat(testData[i]);
			}
			TestRecord testRecord = new TestRecord(new Date().getTime(),attribute, this.K);
			testRecords.add(testRecord);
		}
		in.close();
	}

}
