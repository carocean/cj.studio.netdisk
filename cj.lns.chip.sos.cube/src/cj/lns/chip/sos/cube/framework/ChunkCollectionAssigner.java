package cj.lns.chip.sos.cube.framework;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoCommandException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import cj.studio.ecm.EcmException;
import cj.ultimate.util.StringUtil;

/*
 * {'assigner':'system_fs_assigner','thresholdCount':33333333,'thresholdSize':33333333,'chunkColNum':0}
 * thresholdCount是以记录数作为阀值，thresholdSize是以实际的数据大小作为阀值，当记录阀为-1时表示永远使用第0个块集合，不再扩展。
 * 注意：不必采用锁，因为即便是另一个取出旧名大大不了该文件继续在旧文件块上写。
 */
public class ChunkCollectionAssigner implements IChunkCollectionAssigner {
	final String KEY_CHUNKSCOL_PREFIX = "system_fs_chunks";
	Cube cube;
	MongoCollection<Document> assigner;

	public ChunkCollectionAssigner() {
	}

	@Override
	public String assignChunkColName() {
		Document assigner = findAssinger();
		double thresholdCount = assigner.getDouble("thresholdCount");// 此值为-1表示使用统一一个集合即num=0
		if (thresholdCount == -1) {
			return String.format("%s_0", KEY_CHUNKSCOL_PREFIX);
		}
		double thresholdSize = assigner.getDouble("thresholdSize");
		long num = assigner.getLong("chunkColNum");
		String chunkColName = String.format("%s_%s", KEY_CHUNKSCOL_PREFIX, num);
		double arr[] = getChunkColCount(chunkColName);
		if (arr[0] >= thresholdCount && arr[1] >= thresholdSize) {//两个阀值都溢出才申请新块
			// 如果当前集合超出，则在前序集合中查找是否已有低于此阀的（因为可能被删除，超是靠前越有可能被删的多），如果没有可用的集合，则分配新块集合
			chunkColName = checkOldChunkCols(thresholdCount,thresholdSize, num);
			if (StringUtil.isEmpty(chunkColName)) {
				chunkColName = assignNewChunkCol(num);
			}
		}
		return chunkColName;
	}

	private String checkOldChunkCols(double threshold,double thresholdSize, long num) {
		for (int i = 0; i < num; i++) {
			String chunkColName = String.format("%s_%s", KEY_CHUNKSCOL_PREFIX,
					i);
			double[] arr = getChunkColCount(chunkColName);
			if (arr[0] < threshold&&arr[1]<thresholdSize) {
				return chunkColName;
			}
		}
		return null;
	}

	@Override
	public MongoCollection<Document> assignChunkCol() {
		String name = assignChunkColName();
		return cube.cubedb.getCollection(name);
	}

	private String assignNewChunkCol(long num) {
		// 原号加1，并创建索引，再更新号分配器记录下新的num
		num++;
		String newName = String.format("%s_%s", KEY_CHUNKSCOL_PREFIX, num);
		MongoCollection<Document> col = cube.cubedb.getCollection(newName);
		createIndex(col);
		updateAssigner(num);
		return newName;
	}

	private void updateAssigner(long num) {
		assigner.updateOne(new BasicDBObject("assigner", KEY_ASSIGNER_COLNAME),
				new BasicDBObject("$set",
						new BasicDBObject("chunkColNum", num)));
	}

	private double[] getChunkColCount(String chunkCol) {

		BsonDocument bd = new BsonDocument("collStats",
				new BsonString(chunkCol));
		Document bddoc = null;
		try {
			bddoc = cube.cubedb.runCommand(bd);
		} catch (MongoCommandException e) {
			if (e.getCode() == -1) {
				cube.cubedb.createCollection(chunkCol);
				bddoc = cube.cubedb.runCommand(bd);
			} else {
				throw new EcmException(e);
			}
		}
		double[] arr=new double[2];
		arr[0]= Cube.convertToDouble(bddoc.get("count"));
		arr[1]= Cube.convertToDouble(bddoc.get("size"));
		return arr;
	}

	@Override
	public void init(ICube cube) {
		this.cube = (Cube) cube;
		this.assigner = this.cube.cubedb.getCollection(KEY_ASSIGNER_COLNAME);
		Document one = findAssinger();
		if (one == null) {
			one = new Document();
			one.put("assigner", KEY_ASSIGNER_COLNAME);
			one.put("thresholdCount", cube.config().getChunkColThresholdCount());
			one.put("thresholdSize", cube.config().getChunkColThresholdSize());
			one.put("chunkColNum", 0L);
			assigner.createIndex(new BasicDBObject("assigner", 1));
			assigner.insertOne(one);

			assignNewChunkCol(-1);// 初始化立方体时先分配一个块集合，只所以是-1因为这个方法中会先++，因此正好得0
		}
	}

	private Document findAssinger() {
		FindIterable<Document> find = assigner
				.find(new BasicDBObject("assigner", KEY_ASSIGNER_COLNAME));
		Document one = find.first();
		return one;
	}

	private void createIndex(MongoCollection<Document> chunk) {
		chunk.createIndex(new BasicDBObject("fileId", 1));
		chunk.createIndex(new BasicDBObject("bnum", 1));
		chunk.createIndex(new BasicDBObject("block", 1));
	}
}
