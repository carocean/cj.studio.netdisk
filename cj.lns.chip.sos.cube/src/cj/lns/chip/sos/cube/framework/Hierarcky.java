package cj.lns.chip.sos.cube.framework;

//用来声明级别在层中的次序
public class Hierarcky {
	String name;// 由于定义为一个维一个层，因此与维名同
	Level head;// 有先后顺序的

	public Hierarcky(String name) {
		this.name = name;
	}

	public Level head() {
		return head;
	}
	public boolean isEmpty(){
		return head==null;
	}
	public void setHead(Level level){
		this.head=level;
	}
	public void appendLevel(Level e) {
		if (head == null) {
			head = e;
			return;
		}
		Level tmp = head;
		do {
			if (tmp.nextLevel() == null) {
				tmp.nextLevel(e);
				break;
			}
			tmp = tmp.nextLevel();
		} while (tmp!=null);
	}
	public Level level(int i){
		if(head==null){
			return null;
		}
		Level tmp = head;
		int pos=0;
		do {
			if(pos==i){
				return tmp;
			}
			tmp = tmp.nextLevel();
			pos++;
		} while (tmp!=null);
		return null;
	}
	public Level level(String propName) {
		if (head == null) {
			return null;
		}
		Level tmp = head;
		do {
			if(tmp.property().name.equals(propName)){
				return tmp;
			}
			tmp = tmp.nextLevel();
		} while (tmp!=null);
		return null;
	}
}
