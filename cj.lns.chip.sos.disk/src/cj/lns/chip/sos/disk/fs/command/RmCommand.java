package cj.lns.chip.sos.disk.fs.command;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
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

@CjService(name = "rmCommand")
public class RmCommand extends Command {
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
		if(!line.hasOption("f")&&!line.hasOption("d")){
			System.out.println(String.format("%s缺少参数", indent));
		}
		if (line.hasOption("f")) {
			String file = line.getOptionValue("f").trim();
			if (!file.startsWith("/")) {
				file = String.format("%s/%s", dir.dir.path(), file)
						.replace("//", "/");
			}
			if (!fs.existsFile(file)) {
				System.out.println(String.format("%s不存在文件：%s", indent, file));
			} else {
				try {
					FileInfo f = fs.openFile(file, OpenMode.onlyOpen);
					f.delete(true);
				} catch (FileLockException e) {
					throw new IOException(e);
				}
			}
		}
		if (line.hasOption("d")) {
			String directory = line.getOptionValue("d").trim();
			if (!directory.startsWith("/")) {
				directory = String.format("%s/%s", dir.dir.path(), directory)
						.replace("//", "/");
			}
			DirectoryInfo di = fs.dir(directory);
			if (!di.exists()) {
				System.out.println(
						String.format("%s不存在目录：%s", indent, directory));
			} else {
				di.delete();
			}
		}
	}

	@Override
	public String cmd() {
		return "rm";
	}

	@Override
	public String cmdDesc() {
		return "删除文件";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option file = new Option("f", "file", true, "删除文件");
		options.addOption(file);
		Option dir = new Option("d", "dir", true, "删除目录");
		options.addOption(dir);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
