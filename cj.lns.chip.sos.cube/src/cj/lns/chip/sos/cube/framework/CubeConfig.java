package cj.lns.chip.sos.cube.framework;

public class CubeConfig {
	protected double capacity = -1;// -1表示空间不受限制
	protected double chunkColThresholdCount=-1;//块集合的阀值(阀值是一个集合中的记录个数），在超过此值时将在新块集合上创建。阀值为-1表示使用统一一个集合
	protected double chunkColThresholdSize=8D*1024*1024*1024;//默认8G大小
	protected boolean autoFoundMembers;// 默认为true
	transient protected String dimFile;
	transient private String memberFile;
	String alias;
	String desc;
	
	/**
	 * -1表示空间不受限制，默认为-1
	 * <pre>
	 *
	 * </pre>
	 */
	public CubeConfig() {

	}
	/**
	 *  -1表示空间不受限制，默认为-1
	 * <pre>
	 *
	 * </pre>
	 * @param capacity
	 */
	public CubeConfig(double capacity) {
		this.capacity = capacity;
	}
	public double getChunkColThresholdCount() {
		return chunkColThresholdCount;
	}
	public void setChunkColThresholdCount(double chunkColThresholdCount) {
		this.chunkColThresholdCount = chunkColThresholdCount;
	}
	public double getChunkColThresholdSize() {
		return chunkColThresholdSize;
	}
	public void setChunkColThresholdSize(double chunkColThresholdSize) {
		this.chunkColThresholdSize = chunkColThresholdSize;
	}
	public String alias() {
		return alias;
	}
	public String getDesc() {
		return desc;
	}
	public void setDesc(String desc) {
		this.desc = desc;
	}
	public void alias(String alias) {
		this.alias = alias;
	}
	public double getCapacity() {
		return capacity;
	}

	public void setCapacity(double capacity) {
		this.capacity = capacity;
	}

	public boolean isAutoFoundMembers() {
		return autoFoundMembers;
	}

	public void setAutoFoundMembers(boolean autoFoundMembers) {
		this.autoFoundMembers = autoFoundMembers;
	}

	public void setCoordinateFile(String memberFile) {
		this.memberFile = memberFile;

	}

	public void setDimFile(String dimFile) {
		this.dimFile = dimFile;
	}

	public String getDimFile() {
		return dimFile;
	}

	public String getCoordinateFile() {
		return memberFile;
	}
}
