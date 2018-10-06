package cj.lns.chip.sos.cube.framework;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

/**
 * 块集合分配器
 * <pre>
 * 原因：mongodb一个集合对应一个物理文件，对于文件的存储，如果像电影这样的大文件很快导致物理文件达到10g以上
 * 		因此查询速度变得超慢，越大超慢，建索引都没用，因此应采用多文件块的方式来存储文件系统
 * 原理：在文件创建时向块分配器申请块号，块分配器设置一个物理文件的最大阀，超过此值则分配新块，否则返回当前的块（块就是一个mongodb的集合）
 * 
 * 说明：阀值是一个块集合的记录数，比如10万条之后申请新块
 * 注意：阀值为-1表示使用统一一个集合
 * </pre>
 * @author carocean
 *
 */
public interface IChunkCollectionAssigner {
	static String KEY_ASSIGNER_COLNAME="system_fs_assigner";
	/**
	 * 获取块集合名
	 * <pre>
	 * 如果超过阀值则分配新名，否则返回先前使用的
	 * </pre>
	 * @return
	 */
	String assignChunkColName();
	/**
	 * 阀值
	 * <pre>
	 * 该值的修改只会影响之后的块集合大小，不影响前面已分配的集合
	 * </pre>
	 * @param threshold
	 */
	void init(ICube cube);
	MongoCollection<Document> assignChunkCol();
}
