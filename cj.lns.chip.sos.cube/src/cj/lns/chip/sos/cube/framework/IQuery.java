package cj.lns.chip.sos.cube.framework;

import java.util.List;

/**
 * 自由查询
 * <pre>
 *
 * </pre>
 * @author carocean
 *
 */
public interface IQuery<T>{
	List<IDocument<T>> getResultList();
	IDocument<T> getSingleResult();
	void setParameter(String name, Object value);
	void removeParameter(String name);
	/**
	 * 
	 * <pre>
	 * 注意：统计函数语句中指定列没有意义，如果还是指定了，则会被忽略
	 * </pre>
	 * @return
	 */
	long count();
}
