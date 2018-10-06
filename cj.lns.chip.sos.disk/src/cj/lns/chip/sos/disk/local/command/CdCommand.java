package cj.lns.chip.sos.disk.local.command;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "cdLocalCommand")
public class CdCommand extends Command {
	@CjServiceInvertInjection
	@CjServiceRef(refByName = "localConsole")
	Console localConsole;
	File dir;// 当前的目录

	void check() {
		if (dir == null) {
			String home = System.getProperty("user.home");
			dir = new File(home);
		}
	}

	public void doCommand(CmdLine cl) throws IOException {
		check();
		CommandLine line = cl.line();
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty()){
			String home = System.getProperty("user.home");
			dir = new File(home);
			return;// 回到主目录下
		}
		String indent = (String) cl.prop("indent");
		String arg0 = args.get(0).trim();
		if ("..".equals(arg0)) {// 切换到父目录
			dir = dir.getParentFile() == null ? dir : dir.getParentFile();
		} else if (".".equals(arg0)) {// 不动
			return;
		} else {
			String path = arg0;
			if (!path.startsWith("/")) {// 视为当前子目录或孙目录
				path = String.format("%s/%s", dir.getPath(), path).replace("//",
						"/");
			}
			File d = new File(path);// 切换到子目录
			if (!d.exists()) {
				System.out.println(String.format("%s%s路径不存在", indent, path));
			} else {
				dir = d;
			}
		}
	}

	@Override
	public String cmd() {
		return "cd";
	}

	@Override
	public String cmdDesc() {
		return "进入目录";
	}
	@Override
	protected void dispose() {
//		dir=null;//注释掉的原因是让本地的操作历史保留下来，下次进入时还到本目录
	}
	@Override
	public Options options() {
		Options options = new Options();
		// Option name = new Option("n", "name",true, "网盘名");
		// options.addOption(name);
		// Option u = new Option("u", "user",true, "用户名");
		// options.addOption(u);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
