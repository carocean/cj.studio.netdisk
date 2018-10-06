package cj.lns.chip.sos.disk.fs.command;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "lsCommand")
public class LsCommand extends Command {
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
		if (cl.line().hasOption("l")) {
			printDetail(indent, cl.line());
		} else {
			printNames(indent);
		}

	}

	private void printNames(String indent) {
		List<String> files = dir.dir.listFileNames();
		System.out.println(String.format(
				"文件夹 %s --------------------- 目录名：%s --------------------",
				dir.dir.name(), dir.dir.dirName()));
		int i = 0;
		int colsize = 3;
		for (String file : files) {
			System.out.print(String.format("%s\t\t\t", file));
			i++;
			int pos = i % colsize;
			if (pos == 0) {
				System.out.println();
			}
		}
		i = 0;
		System.out.println("\r\n目录:");
		List<DirectoryInfo> dirs = dir.dir.listDirs();
		for (DirectoryInfo d : dirs) {
			System.out.print(String.format("%s\t\t\t", d.dirName()));
			i++;
			int pos = i % colsize;
			if (pos == 0) {
				System.out.println();
			}
		}
		System.out.println();
	}

	private void printDetail(String indent, CommandLine line) {
		System.out.println(String.format(
				"文件夹 %s --------------------- 目录名：%s --------------------",
				dir.dir.name(), dir.dir.dirName()));
		System.out
				.println(String.format("\t%s类型\t文件大小|目录文件数\t创建日期\t名称", indent));
		List<FileInfo> files = dir.dir.listFiles();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (FileInfo file : files) {
			String date = format.format(file.createDate());
			if (line.hasOption("lr")) {
				System.out.println(String.format("\t%s-F\t\t%s/%s\t\t%s\t%s",
						indent,file.dataLength(), file.spaceLength(), date, file.name()));
			} else {
				System.out.println(String.format("\t%s-F\t\t%s\t\t%s\t%s",
						indent, file.spaceLength(), date, file.name()));
			}
		}
		List<DirectoryInfo> dirs = dir.dir.listDirs();

		for (DirectoryInfo d : dirs) {
			String date = format.format(d.createDate());
			System.out.println(String.format("\t%s-D\t\t%s\t\t%s\t%s", indent,
					d.fileCount(), date, d.dirName()));
		}
	}

	@Override
	public String cmd() {
		return "ls";
	}

	@Override
	public String cmdDesc() {
		return "列出文件";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option l = new Option("l", "list", false, "列出明细");
		options.addOption(l);
		Option ld = new Option("lr", "listreal", false, "列出实际数据大小，统计稍慢");
		options.addOption(ld);
		// Option f = new Option("fr", "file", false, "查看一个文件的信息");
		// options.addOption(f);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
