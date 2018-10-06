package cj.lns.chip.sos.disk.local;

import java.io.IOException;
import java.util.Map;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.usf.UsfConsole;
import cj.studio.ecm.annotation.CjService;
@CjService(name="localConsole")
public class LocalConsole extends Console {
	FileSystem fs;
	DirectoryInfo dir;
	@Override
	protected String prefix(MongoClient client, Object... target) {
		String indent=(String)target[1];
		return UsfConsole.COLOR_CMDPREV + indent+ dir.path()+" >local >"
				+ UsfConsole.COLOR_CMDLINE;
	}
	@Override
	public void monitor(MongoClient client, Object... target)
			throws IOException {
		dir=(DirectoryInfo)target[0];
		fs=(FileSystem)target[2];
		super.monitor(client, target);
	}
	@Override
	protected void beforDoCommand(Command cmd, CmdLine cl) {
		cl.prop("dir",dir);
		cl.prop("fs",fs);
		String indent=String.format("%s%s",cl.prop("indent"),cl.prop("indent"));
		cl.prop("indent",indent);
	}
	@Override
	protected void printMan(MongoClient client, Object[] target,Map<String, Command> cmds) {
		System.out.println(prefix(client, target)+"本地文件系统指令集");
		super.printMan(client, target, cmds);
	}

	@Override
	protected boolean exit(String cmd) {
		if("close".equals(cmd)){
			return true;
		}
		return false;
	}

}
