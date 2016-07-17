package knn.kvtype;

import util.Utils;

/**
 * @author begin
 * @date 2016-7-15
 * 与测试记录关系的训练数据记录
 * */
public class TrainRecord implements Comparable<TrainRecord> {
	private float distance; // 与对应测试数据集的距离
	private String type; // 对应类别

	public TrainRecord(float distance, String type) {
		this.distance = distance;
		this.type = type;
	}

	public TrainRecord(TestRecord testRecord, String[] line) {
		// 初始化 属性、距离、类别
		float[] attributes = new float[line.length - 1];
		for (int i = 0; i < line.length - 1; i++) {
			attributes[i] = Float.valueOf(line[i]);
		}
		this.distance = Utils.Outh(testRecord.getAttributes(), attributes);
		this.type = line[line.length - 1];
	}

	public float getDistance() {
		return distance;
	}

	public void setDistance(float distance) {
		this.distance = distance;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public int compareTo(TrainRecord trainRecord) {
		if (this.distance > trainRecord.distance)
			return 1;
		else if (this.distance < trainRecord.distance)
			return -1;
		else
			return 0;
	}
}
