package knn;

import knn.kvtype.TestRecord;
import knn.mr.KnnCombiner;
import knn.mr.KnnMapper;
import knn.mr.KnnReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * knn算法实现
 * 调用参数
 * args[0]：训练数据路径
 * args[1]:测试数据路径
 * args[2]:最近邻算法k
 * 
 * 输入训练数据格式：
 * x1,x2,x3,...xn,Lable
 * 
 * 输入预测数据格式
 * x1,x2,x3,....xn
 * 
 * 输出数据格式:
 * x1,x2,x3,...xn,Lable
 * 
 * 由大量的历史数据预测新的数据。
 * 训练集一般是比较大，而测试集比较小，所以进入map的是训练集。而把测试集放入内存。
 * @author 许国杰
 */
public class KnnDriver extends Configured implements Tool{
	
	public static void main(String[] args) throws Exception {
		int res =0;
		try
        {
			res = ToolRunner.run(new Configuration(), new KnnDriver(), args);
        }
		catch(Exception e){
			e.printStackTrace();
		}
        System.exit(res);
	}

    public int run(String[] args) throws Exception {
    	//解析参数 训练数据集
    	String trainDataPath = args[0];
    	//解析参数 测试数据路径
    	String testFilePath = args[1];
    	//解析参数 输出结果路径
    	String outPutFilePath = args[2];
    	//最近邻算法K
    	int K = Integer.parseInt(args[3]);
    	
        Configuration conf = getConf();
        conf.setInt("K", K);
        conf.set("TestFilePath", testFilePath);

		Job job = Job.getInstance(conf,"Knn-T Model Job");
        job.setJarByClass(KnnDriver.class);
        job.setMapperClass(KnnMapper.class);
        job.setCombinerClass(KnnCombiner.class);
        job.setReducerClass(KnnReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(TestRecord.class);
        
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        
        Path out =new Path(outPutFilePath);
        out.getFileSystem(conf).delete(out, true);
        
        FileInputFormat.addInputPath(job, new Path(trainDataPath));
        FileOutputFormat.setOutputPath(job, out);
        int res = job.waitForCompletion(true) ? 0 : 1;
        return res;
    }
}
