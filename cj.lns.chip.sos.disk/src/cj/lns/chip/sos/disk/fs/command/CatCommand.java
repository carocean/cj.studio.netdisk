package cj.lns.chip.sos.disk.fs.command;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.IReader;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "catCommand")
public class CatCommand extends Command {
	FileSystem fs;
	@CjServiceInvertInjection
	@CjServiceRef(refByName = "fsConsole")
	Console fsConsole;
	@CjServiceRef(refByName = "cdCommand")
	CdCommand dir;

	public void doCommand(CmdLine cl) throws IOException {
		fs = (FileSystem) cl.prop("fs");
		String indent = (String) cl.prop("indent");
		if (dir.dir == null) {
			// 默认执行cd /
			try {
				CommandLine line;
				GnuParser parser = new GnuParser();
				String[] args = new String[] { "/" };
				line = parser.parse(dir.options(), args);
				CmdLine cd = new CmdLine("cd", line);
				cd.copyPropsFrom(cl);
				dir.doCommand(cd);
			} catch (ParseException e) {
				throw new EcmException(e);
			}
		}
		CommandLine line = cl.line();
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty()) {
			System.out.println(String.format("%scat 需要文件路径参数", indent));
			return;
		}
		String path = args.get(0).trim();
		if(!path.startsWith("/")){
			path=String.format("%s/%s", dir.dir.path(),path).replace("//", "/");
		}
		if (!fs.existsFile(path)) {
			System.out.println(String.format("%s%s 文件不存在", indent, path));
			return;
		}
		try {
			FileInfo file = fs.openFile(path);
			String cnt = "";
			if (!line.hasOption("end")) {
				if (line.hasOption("pos")) {
					cnt = new String(file
							.reader(Long.valueOf(line.getOptionValue("pos")))
							.readFully());
				} else {
					cnt = new String(file.reader(0).readFully());
				}
			}else{
				long pos=0;
				if(line.hasOption("pos")){
					pos=Long.valueOf(line.getOptionValue("pos"));
				}
				long end=Long.valueOf(line.getOptionValue("end"));
				cnt=read(file.reader(pos),pos,end);
			}
			System.out.println(cnt);
		} catch (Exception e) {
			System.out.println(String.format("%s错误：%s", indent, e));
		}
	}

	private String read(IReader reader,long pos, long end) {
		if((end-pos)/1024/1024>1){
			throw new EcmException("每次查看的大小不能超出1m，调整pos,end参数");
		}
		ByteArrayOutputStream out=new ByteArrayOutputStream();
		for(long i=pos;i<end;i++){
			int b=reader.read();
			if(b==-1){
				break;
			}
			out.write(b);
		}
		return new String(out.toByteArray());
	}

	@Override
	public String cmd() {
		return "cat";
	}

	@Override
	public String cmdDesc() {
		return "查看文件内容。例：cat /my/我日你妈比啊.doc";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option pos = new Option("pos", "postion", true, "从文件的指定偏移位置开始查看，注意：如果文件超出10m大小，一定要带上end参数");
		options.addOption(pos);
		Option end = new Option("end", "end", true, "读取到指定位置");
		options.addOption(end);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
