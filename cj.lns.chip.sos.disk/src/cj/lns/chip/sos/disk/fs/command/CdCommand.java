package cj.lns.chip.sos.disk.fs.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "cdCommand")
public class CdCommand extends Command {
	@CjServiceInvertInjection
	@CjServiceRef(refByName = "fsConsole")
	Console fsConsole;
	DirectoryInfo dir;// 当前的目录,注意：在退出目录系统时必须设此为null否则会导成几个空间的文件系统穿换，原因是其它命令要使用此值，而此值只在dir不是空时才设，因此在此命令中释放即可
	public void cdRootDir(FileSystem fs) {
		dir = fs.dir("/");
		dir.refresh();
	}
	public void doCommand(CmdLine cl) throws IOException {
		FileSystem fs = (FileSystem) cl.prop("fs");
		if (dir == null) {
			dir = fs.dir("/");
			dir.refresh();
		}
		CommandLine line = cl.line();
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty())
			return;// 即目录不动
		String indent=(String)cl.prop("indent");
		String arg0 = args.get(0).trim();
		if ("..".equals(arg0)) {//切换到父目录
			DirectoryInfo parent=dir.parent();
			if(parent!=null){
				dir=parent;
			}
		} else if (".".equals(arg0)) {//不动
			return;
		} else {
			String path=arg0;
			if(!path.startsWith("/")){//视为当前子目录或孙目录
				path=String.format("%s/%s", dir.path(),path).replace("//", "/");
			}
			DirectoryInfo d = fs.dir(path);// 切换到子目录
			if(!d.exists()){
				System.out.println(String.format("%s%s路径不存在", indent,path));
			}else{
				d.refresh();
				dir=d;
			}
		}
	}

	@Override
	public String cmd() {
		return "cd";
	}

	@Override
	public String cmdDesc() {
		return "进入目录。说明：在windows下如需切换磁盘卷前面必须加上/号，如切换到D盘，则：/D:/，如果后面的/省略虽然也切换至D盘，但列出空目录，需要再使用：cd / 才进入D盘的根";
	}
	@Override
	protected void dispose() {
		dir=null;// 当前的目录,注意：在退出目录系统时必须设此为null否则会导成几个空间的文件系统穿换，原因是其它命令要使用此值，而此值只在dir不是空时才设，因此在此命令中释放即可
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
