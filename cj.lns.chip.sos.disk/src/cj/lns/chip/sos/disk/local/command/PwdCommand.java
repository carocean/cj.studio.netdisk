package cj.lns.chip.sos.disk.local.command;

import java.io.IOException;

import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "pwdLocalCommand")
public class PwdCommand extends Command {
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
//		CommandLine line = cl.line();
		System.out.println(String.format("%s%s", indent, local.dir.getPath()));
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
		// Option l = new Option("l", "list", false, "列出明细");
		// options.addOption(l);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
