package cj.lns.chip.sos.cube.framework;
//事务中的记录仍然可以读出，但具有isPendding=true字段，供应用层比如使用新记录的id，统计该表时提供两个方法，一个是过滤掉事务记录，一个是不过滤掉事务记录，但如果事务失败会去除
public interface ITranscation {
	String id();
	void setContainsPenddingRows(boolean arg);
	boolean isContainsPenddingRows();
	void commit();
	boolean isActive();
	void rollback();
}
