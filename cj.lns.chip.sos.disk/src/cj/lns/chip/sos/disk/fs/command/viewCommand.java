package cj.lns.chip.sos.disk.fs.command;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cj.lns.chip.sos.cube.framework.Coordinate;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.OpenMode;
import cj.lns.chip.sos.cube.framework.lock.FileLockException;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "viewCommand")
public class viewCommand extends Command {
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
			System.out.println(String.format("%s没有指定文件", indent));
			return;// 即目录不动
		}
		String file = args.get(0).trim();
		if (!file.startsWith("/")) {
			file = String.format("%s/%s", dir.dir.path(), file).replace("//",
					"/");
		}
		if (!fs.existsFile(file)) {
			System.out.println(String.format("%s不存在文件：%s", indent, file));
		} else {
			try {
				FileInfo f = fs.openFile(file, OpenMode.onlyOpen);
				viewFile(f,indent);
			} catch (FileLockException e) {
				throw new IOException(e);
			}
		}
	}

	private void viewFile(FileInfo f,String indent) {
		
		System.out.println(String.format("%s%s", indent,f.fullName()));
		System.out.println(String.format("%s\t文件号：\t%s",indent, f.phyId()));	
		System.out.println(String.format("%s\t空间大小：\t%s",indent, f.spaceLength()));	
		System.out.println(String.format("%s\t数据大小：\t%s",indent, f.dataLength()));	
		System.out.println(String.format("%s\t坐标：",indent));
		Set<String> set=f.enumCoordinate();
		for(String dimName:set){
			Coordinate coord=f.coordinate(dimName);
			System.out.println(String.format("%s\t\t%s:%s",indent, dimName,coord.toPath()));	
		}
	}

	@Override
	public String cmd() {
		return "view";
	}

	@Override
	public String cmdDesc() {
		return "查看一个文件的信息.例：view /xx/xx.file";
	}

	@Override
	public Options options() {
		Options options = new Options();
		// Option l = new Option("l", "list", false, "列出明细");
		// options.addOption(l);
		// Option f = new Option("f", "file", false, "查看一个文件的信息");
		// options.addOption(f);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
