package cj.lns.chip.sos.cube.framework;

/**
 * 在维度上的刻度
 * 
 * <pre>
 * 根据坐标的的nextLevel,parent自动与层级的对应
 * 
 * 成员与坐标区别：
 * 属性的数据是成员，方向是往下，因此往下的是成员
 * 在一个维度上水平的是坐标也是刻度
 * </pre>
 * 
 * @author carocean
 *
 */
public class Coordinate {
	private String propName;
	private Object value;
	Coordinate parentLevel;
	private Coordinate nextLevel;// 根据维度的层级设置下级，如果在保存成员时不符合层级要求，则报异常

	public Coordinate(String propName, Object value) {
		this.value = value;
		this.propName = propName;
	}

	public String propName() {
		return propName;
	}

	public Coordinate parentLevel() {
		return parentLevel;
	}

	public Object value() {
		return value;
	}

	public Coordinate nextLevel(Coordinate nextLevel) {
		this.nextLevel = nextLevel;
		nextLevel.parentLevel = this;
		return nextLevel;
	}

	public Coordinate nextLevel() {
		return nextLevel;
	}

	public Coordinate goHead() {
		Coordinate tmp = this;
		while (tmp != null) {
			if (tmp.parentLevel == null) {
				return tmp;
			}
			tmp = tmp.parentLevel;
		}
		return this;
	}

	public Coordinate goLast() {
		Coordinate tmp = this;
		while (tmp != null) {
			if (tmp.nextLevel == null) {
				return tmp;
			}
			tmp = tmp.nextLevel;
		}
		return this;
	}

	public Coordinate truncateAndGoHead() {
		Coordinate tmp = goHead();
		Coordinate head = tmp.copy();
		Coordinate htmp = head;
		while (tmp != null) {
			if (tmp.equals(this)) {
				break;
			}
			if (tmp.nextLevel == null) {
				break;
			}
			htmp = htmp.nextLevel(tmp.nextLevel.copy());
			tmp = tmp.nextLevel;
		}
		return head;
	}

	/**
	 * 拷贝一个，但不包含上下级坐标
	 * 
	 * <pre>
	 *
	 * </pre>
	 * 
	 * @return
	 */
	public Coordinate copy() {
		Coordinate coord = new Coordinate(propName, value);
		return coord;
	}

	/**
	 * 深表复制，所有子级坐标
	 * 
	 * <pre>
	 * 注：本方法是从尾向头复制，且复制后定位到头坐标
	 * </pre>
	 * 
	 * @return
	 */
	public Coordinate depCopyFromEnd() {
		Coordinate tmp = this;
		Coordinate head=tmp.copy();
		while (tmp != null) {
			tmp=tmp.parentLevel;
			if (tmp == null) {
				break;
			}
			Coordinate copy=tmp.copy();
			head.parentLevel=copy;
			copy.nextLevel=head;
			head=copy;
		}
		return head;
	}

	public String toPath() {
		String ret = "";
		Coordinate tmp = this;
		while (tmp != null) {
			ret = String.format("%s/%s", ret, tmp.value);
			if (tmp.nextLevel == null) {
				break;
			}
			tmp = tmp.nextLevel;
		}
		return ret;
	}
}
