package knn.kvtype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * @author 许国杰
 * @date 2016-7-15
 */
public class TestRecord implements Writable {
	// 属性数组
	private float[] attributes;

	// 最临近训练记录
	private List<TrainRecord> nearestTrainRecord = new ArrayList<TrainRecord>();

	// KNN 算法K
	private int K;

	public TestRecord(){}
	public TestRecord(float[] attributes, int K) {
		this.attributes = attributes;
		this.K = K;
	}

	/*
	 * 增加最临近训练记录
	 */
	public void addTrainRecord(TrainRecord trainRecord) {
		if (nearestTrainRecord.size() < K) {
			nearestTrainRecord.add(trainRecord);
		} else {
			// 对列表中的记录排序
			if (nearestTrainRecord.get(K - 1).compareTo(trainRecord) > 0) {
				nearestTrainRecord.remove(K - 1);
				nearestTrainRecord.add(trainRecord);
			}
		}
		Collections.sort(nearestTrainRecord); // 保持列表中的记录是有序的
	}

	/*
	 * 相同的一条测试数据 合并其最临近训练记录列表
	 */
	public void merge(TestRecord testRecord) {
		for (TrainRecord t : testRecord.nearestTrainRecord) {
			this.addTrainRecord(t);
		}
	}

	/**
	 * 得到最临近训练记录中的多数据类
	 */
	public String getMostType() {
		int count = 1;
		int longest = 0;
		String most = "";
		for (int i = 0; i < nearestTrainRecord.size() - 1; i++) {
			if (nearestTrainRecord.get(i).getType()
					.equals(nearestTrainRecord.get(i + 1).getType())) {
				count++;
			} else {
				count = 1;// 如果不等于，就换到了下一个数，那么计算下一个数的次数时，count的值应该重新赋值为一
				continue;
			}
			if (count > longest) {
				most = nearestTrainRecord.get(i).getType();
				longest = count;
			}
		}
		return most;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(attributes.length);
		for (int i = 0; i < attributes.length; i++) {
			out.writeFloat(attributes[i]);
		}
		out.writeInt(nearestTrainRecord.size());
		for (int i = 0; i < nearestTrainRecord.size(); i++) {
			out.writeFloat(nearestTrainRecord.get(i).getDistance());
			out.writeUTF(nearestTrainRecord.get(i).getType());
		}
		out.writeInt(K);
	}

	public void readFields(DataInput in) throws IOException {
		int attributesLength = in.readInt();
		attributes = new float[attributesLength];
		for (int i = 0; i < attributesLength; i++) {
			attributes[i] = in.readFloat();
		}
		int nearestTrainRecordSize = in.readInt();
		this.nearestTrainRecord = new ArrayList<TrainRecord>();
		for (int i = 0; i < nearestTrainRecordSize; i++) {
			this.nearestTrainRecord.add(new TrainRecord(in.readFloat(), in
					.readUTF()));
		}
		this.K = in.readInt();
	}
	
	public int getK() {
		return K;
	}

	public void setK(int k) {
		K = k;
	}

	public float[] getAttributes() {
		return attributes;
	}

	public void setAttributes(float[] attributes) {
		this.attributes = attributes;
	}

	public List<TrainRecord> getNearestTrainRecord() {
		return nearestTrainRecord;
	}

	public void setNearestTrainRecord(List<TrainRecord> nearestTrainRecord) {
		this.nearestTrainRecord = nearestTrainRecord;
	}

}
