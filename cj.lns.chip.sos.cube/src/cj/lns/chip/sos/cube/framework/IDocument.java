package cj.lns.chip.sos.cube.framework;

/**
 * 元组文档
 * <pre>
 * 一个文档由多个维的成员（在一个文档内，一个维度仅能出现一次）组成
 * 一个文档有且仅有一个元组对象
 * </pre>
 * @author carocean
 *
 * @param <T>
 */
public interface IDocument<T> {
	String docid();
	String[] enumCoordinate();
	/**
	 * 一个文档中一个维度只充许使用一次，
	 * <pre>
	 * 如果已存在指定的维度，则异常
	 * 如果成员不符合维度结构的约束，则异常
	 * </pre>
	 * @param dim
	 * @param coordinate 一个成员链
	 */
	void addCoordinate(String dim,Coordinate coordinate);
	boolean existsCoordinate(String dim);
	void removeCoordinate(String dim);
	Coordinate coordinate(String dim);
	T tuple();
	boolean isPendding();
}
