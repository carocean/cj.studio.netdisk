package cj.lns.chip.sos.disk.netdisk;

import java.io.IOException;
import java.util.Map;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.INetDisk;
import cj.lns.chip.sos.disk.usf.UsfConsole;
import cj.studio.ecm.annotation.CjService;
@CjService(name="diskConsole")
public class DiskConsole extends Console {
	INetDisk disk;
	@Override
	protected String prefix(MongoClient client, Object... target) {
		String indent=(String)target[1];
		return UsfConsole.COLOR_CMDPREV + indent+disk.name()+ " >"
				+ UsfConsole.COLOR_CMDLINE;
	}
	@Override
	public void monitor(MongoClient client, Object... target)
			throws IOException {
		disk=(INetDisk)target[0];
		super.monitor(client, target);
	}
	@Override
	protected void beforDoCommand(Command cmd, CmdLine cl) {
		cl.prop("disk",disk);
		String indent=String.format("%s%s",cl.prop("indent"),cl.prop("indent"));
		cl.prop("indent",indent);
	}
	@Override
	protected void printMan(MongoClient client, Object[] target,Map<String, Command> cmds) {
		System.out.println(prefix(client, target)+"网盘指令集");
		super.printMan(client, target, cmds);
	}

	@Override
	protected boolean exit(String cmd) {
		if("close".equals(cmd)){
			disk.close();
			return true;
		}
		return false;
	}

}
