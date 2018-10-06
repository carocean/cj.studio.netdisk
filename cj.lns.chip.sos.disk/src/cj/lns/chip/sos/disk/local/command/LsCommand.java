package cj.lns.chip.sos.disk.local.command;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "lsLocalCommand")
public class LsCommand extends Command {
	DirectoryInfo dir;
	@CjServiceInvertInjection
	@CjServiceRef(refByName = "localConsole")
	Console localConsole;
	@CjServiceRef(refByName = "cdLocalCommand")
	CdCommand local;

	public void doCommand(CmdLine cl) throws IOException {
		local.check();
		dir = (DirectoryInfo) cl.prop("dir");
		String indent = (String) cl.prop("indent");
		CommandLine line = cl.line();
		if (line.hasOption("l")) {
			printDetail(indent);
		} else {
			printNames(indent);
		}
	}

	private void printNames(String indent) {
		File[] files = local.dir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return pathname.isFile();
			}
		});
		int i = 0;
		int colsize = 3;
		for (File file : files) {
			System.out.print(String.format("%s\t\t\t", file.getName()));
			i++;
			int pos = i % colsize;
			if (pos == 0) {
				System.out.println();
			}
		}
		i = 0;
		File[] dirs = local.dir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory();
			}
		});
		for (File d : dirs) {
			System.out.print(String.format("%s\t\t\t", d.getName()));
			i++;
			int pos = i % colsize;
			if (pos == 0) {
				System.out.println();
			}
		}
		System.out.println();
	}

	private void printDetail(String indent) {
		System.out
				.println(String.format("\t%s类型\t文件大小\t创建日期\t名称", indent));
		File[] files = local.dir.listFiles(new FileFilter() {
			
			@Override
			public boolean accept(File pathname) {
				return pathname.isFile();
			}
		});
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		for (File file : files) {
			String date = format.format(file.lastModified());
			System.out.println(String.format("\t%s-F\t\t%s\t\t%s\t%s", indent,
					file.length(), date, file.getName()));
		}
		File[] dirs = local.dir.listFiles(new FileFilter() {
			
			@Override
			public boolean accept(File pathname) {
				return pathname.isDirectory();
			}
		});

		for (File d : dirs) {
			String date = format.format(d.lastModified());
			System.out.println(String.format("\t%s-D\t\t%s\t\t%s\t%s", indent,
					d.length(), date, d.getName()));
		}
	}

	@Override
	public String cmd() {
		return "ls";
	}

	@Override
	public String cmdDesc() {
		return "查看本地文件系统。";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option l = new Option("l", "list", false, "显示明细");
		options.addOption(l);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
