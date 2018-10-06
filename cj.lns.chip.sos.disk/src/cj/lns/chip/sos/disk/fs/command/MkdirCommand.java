package cj.lns.chip.sos.disk.fs.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "mkdirCommand")
public class MkdirCommand extends Command {
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
		if (args.isEmpty())
			{System.out.println("必须指定目录名，用法：mkdir path -n alias");
			return;}
		String path = args.get(0).trim();
		if (!path.startsWith("/")) {
			path = String.format("%s/%s", dir.dir.path(), path).replace("//",
					"/");
		}
		DirectoryInfo di = fs.dir(path);
		if (di.exists()) {
			System.out.println(String.format("%s目录已存在：%s", indent, di.path()));
		} else {
			if (line.hasOption("n")) {
				di.mkdir(line.getOptionValue("n").trim());
			} else {
				di.mkdir(di.dirName());
			}
		}

	}

	@Override
	public String cmd() {
		return "mkdir";
	}

	@Override
	public String cmdDesc() {
		return "创建目录 mkdir  /crops/test -n 我的测试";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option file = new Option("n", "name", true, "文件夹名");
		options.addOption(file);
//		Option dir = new Option("d", "dir", true, "目录路径");
//		options.addOption(dir);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
