package cj.lns.chip.sos.disk.fs;

import java.io.IOException;
import java.util.Map;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.fs.command.CdCommand;
import cj.lns.chip.sos.disk.usf.UsfConsole;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;

@CjService(name = "fsConsole")
public class FsConsole extends Console {
	FileSystem fs;

	@Override
	protected String prefix(MongoClient client, Object... target) {
		String indent = (String) target[1];
		return UsfConsole.COLOR_CMDPREV + indent + "fs >"
				+ UsfConsole.COLOR_CMDLINE;
	}

	@Override
	public void monitor(MongoClient client, Object... target)
			throws IOException {
		fs = (FileSystem) target[0];
		if(!super.commands.containsKey("cd")){
			throw new EcmException("进入文件系统失败，原因缺少：cd命令");
		}
		CdCommand cd = (CdCommand) super.commands.get("cd");
		cd.cdRootDir(fs);
		super.monitor(client, target);
	}

	@Override
	protected void beforDoCommand(Command cmd, CmdLine cl) {
		cl.prop("fs", fs);
		String indent = String.format("%s%s", cl.prop("indent"),
				cl.prop("indent"));
		cl.prop("indent", indent);
	}

	@Override
	protected void printMan(MongoClient client, Object[] target,
			Map<String, Command> cmds) {
		System.out.println(prefix(client, target) + "存储空间指令集");
		super.printMan(client, target, cmds);
	}

	@Override
	protected boolean exit(String cmd) {
		if ("close".equals(cmd)) {
			return true;
		}
		return false;
	}

}
