package cj.lns.chip.sos.disk.fs.command;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "pwdCommand")
public class PwdCommand extends Command {
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
		System.out.println(String.format("%s%s", indent,dir.dir.path()));

	}

	@Override
	public String cmd() {
		return "pwd";
	}

	@Override
	public String cmdDesc() {
		return "显示当前路径";
	}

	@Override
	public Options options() {
		Options options = new Options();
//		Option l = new Option("l", "list", false, "列出明细");
//		options.addOption(l);
//		Option c = new Option("c", "count", true, "包括统计信息");
//		options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
