package util;
public class Utils {
	//计算两个向量的欧氏距离
	public static float Outh(float[] testData, float[] inData) {
		float distance =0.0f;
		for(int i=0;i<testData.length;i++){
			distance += (testData[i]-inData[i])*(testData[i]-inData[i]);
		}
		distance = (float)Math.sqrt(distance);
		return distance;
	}
}
