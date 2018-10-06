package cj.lns.chip.sos.cube.framework;

/**
 * 属性是维度的字段
 * 
 * <pre>
 * 
 * 在传统的MDX中，属性的值往往来自于其它数据源，所以定义较为复杂，而在
 * 本架构中，属性就是一个字段，因此用一个对象来描述，它的别名、类型等
 * </pre>
 * 
 * @author carocean
 *
 */
public class Property {
	String name;
	String alias;
	String dataType;
	public Property() {
		// TODO Auto-generated constructor stub
	}
	
	public Property(String name, String alias, String dataType) {
		super();
		this.name = name;
		this.alias = alias;
		this.dataType = dataType;
	}

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAlias() {
		return alias;
	}

	public String getDataType() {
		return dataType;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
}
