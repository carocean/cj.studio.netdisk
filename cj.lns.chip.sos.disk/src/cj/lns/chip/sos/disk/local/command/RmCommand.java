package cj.lns.chip.sos.disk.local.command;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.ultimate.util.FileHelper;

@CjService(name = "rmLocalCommand")
public class RmCommand extends Command {
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
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty()) {
			throw new EcmException("缺少参数，格式：rm xxx");
		}
		String file = args.get(0).trim();
		if (!file.startsWith(File.separator)) {
			file = String.format("%s%s%s", local.dir.getPath(), File.separator,
					file);
		}
		File f = new File(file);
		if (!f.exists()) {
			System.out.println(String.format("%s目录或文件不存在：%s", indent, file));
		} else {
			FileHelper.deleteDir(f);
		}
	}

	@Override
	public String cmd() {
		return "rm";
	}

	@Override
	public String cmdDesc() {
		return "删除本地文件和目录。例：rm 目录  rm 文件";
	}

	@Override
	public Options options() {
		Options options = new Options();
		// Option l = new Option("f", "file", false, "删除指定文件");
		// options.addOption(l);
		// Option c = new Option("d", "dir", true, "删除指定文件或目录");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
