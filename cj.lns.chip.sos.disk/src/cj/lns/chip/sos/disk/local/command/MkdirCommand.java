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
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "mkdirLocalCommand")
public class MkdirCommand extends Command {
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
		if (args.isEmpty()){
			return;// 回到主目录下
		}
		String dirPath=args.get(0);
		if(!dirPath.startsWith(File.separator)){
			dirPath = String.format("%s%s%s", local.dir.getPath(),
					File.separator, dirPath);
		}
		File dir=new File(dirPath);
		if(!dir.exists()){
			dir.mkdirs();
		}else{
			System.out.println(String.format("%s目录已存在：%s", indent, dirPath));
		}
	}

	@Override
	public String cmd() {
		return "mkdir";
	}

	@Override
	public String cmdDesc() {
		return "创建本地目录。例：mkdir 路径";
	}

	@Override
	public Options options() {
		Options options = new Options();
//		Option l = new Option("f", "file", false, "删除指定文件");
//		options.addOption(l);
//		Option c = new Option("d", "dir", true, "删除指定目录");
//		options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
