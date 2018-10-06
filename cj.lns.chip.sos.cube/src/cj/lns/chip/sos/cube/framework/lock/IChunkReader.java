package cj.lns.chip.sos.cube.framework.lock;

import cj.lns.chip.sos.cube.framework.Chunk;

public interface IChunkReader {

	Chunk read(String fileId, long blockNum);

	void updateBlockLock(long bnum, long blockHead);

}
