package cj.lns.chip.sos.cube.test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestFile {
	Offset offset;
	Offset end;
	byte[] block;
	long usedsize=8096;
	FileInputStream in;
	public TestFile(FileInputStream in) {
		this.in=in;
	}
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FileInputStream in =new FileInputStream("/Users/carocean/Downloads/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字].mkv");
		FileOutputStream out =new FileOutputStream("/Users/carocean/Downloads/[电影天堂www.dy2018.com]鬼玩人之阿什斗鬼第一季第08集[中英双字]3.mkv");
		TestFile f=new TestFile(in);
		f.offset=f.new Offset(0L, 0);
		f.end=f.new Offset(in.available()/8096, in.available()%8096);
		int read=0;
		byte[] b=new byte[256*1024];
		int total=0;
		while((read=in.read(b, 0, b.length))>-1){
			out.write(b,0,read);
			total+=read;
		}
		System.out.println(total);
		f.in.close();
		out.close();
	}
	public synchronized int read(byte[] b, int off, int len) throws IOException {
		int readtimes=0;
		for(int i=off;i<len-off;i++){
			int d=read();
			if(d==-1)return d;
			b[i]=(byte)d;
			readtimes++;
		}
		return readtimes;
	}

	// 字节转整数(byte&0xFF)反转（byte）int，如果 是-1表示结束
	private synchronized int read() throws IOException {
		if(offset.equals(end)){
			return -1;
		}
		if (block == null ) {
			int t=in.read(block);
			if(t!=-1){
				usedsize=t;
			}
		}
		byte[] data = block;
		if (offset.offset < usedsize) {
			byte b = data[offset.offset];
			offset.offset++;
			return b & 0xFF;
		}else{
			offset.blockNum++;
			offset.offset=0;
			block=null;
			return read();
		}
		
	}
	class Offset {
		public Offset(long blockNum, int offset) {
			this.blockNum = blockNum;
			this.offset = offset;
		}

		long blockNum;
		int offset;

		@Override
		public boolean equals(Object obj) {
			Offset off = (Offset) obj;
			return blockNum == off.blockNum && offset == off.offset;
		}
	}
}
