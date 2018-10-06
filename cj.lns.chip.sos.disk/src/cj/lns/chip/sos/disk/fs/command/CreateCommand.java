package cj.lns.chip.sos.disk.fs.command;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.OpenMode;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "createCommand")
public class CreateCommand extends Command {
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
		if (!line.hasOption("f")) {
			System.out.println("必须参数：f");
		}
		String mode = "auto";
		if (line.hasOption("m")) {
			mode = line.getOptionValue("m");
		}
		String file = line.getOptionValue("f").trim();
		if (!file.startsWith("/")) {
			file = String.format("%s/%s", dir.dir.path(), file).replace("//",
					"/");
		}
		try {
			if (!fs.existsFile(file)) {
				fs.openFile(file, OpenMode.createNew);
			} else {
				if ("auto".equals(mode)) {
					FileInfo fi=fs.openFile(file,OpenMode.openOrNew);
					System.out.println(String.format("%s文件已存在：%s",indent, fi.fullName()));
				} else if ("force".equals(mode)) {
					fs.openFile(file, OpenMode.createNew);
				}else{
					System.out.println(String.format("%s不支持的参数值mode=%s",indent, mode));
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public String cmd() {
		return "create";
	}

	@Override
	public String cmdDesc() {
		return "创建新文件";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option file = new Option("f", "file", true, "要创建的文件名");
		options.addOption(file);
		Option dir = new Option("m", "mode", true,
				"创建的模式：foce强制创建，如果存在则文件置空；auto存在则不创建，不存在则创建,默认是auto");
		options.addOption(dir);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
