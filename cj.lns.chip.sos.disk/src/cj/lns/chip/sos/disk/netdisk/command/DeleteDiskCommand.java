package cj.lns.chip.sos.disk.netdisk.command;

import java.io.IOException;

import org.apache.commons.cli.Options;

import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.ConsoleEditor;
import cj.lns.chip.sos.disk.INetDisk;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "deleteDiskCommand")
public class DeleteDiskCommand extends Command {

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "diskConsole")
	Console diskConsole;

	public void doCommand(CmdLine cl) throws IOException {
		INetDisk disk = (INetDisk) cl.prop("disk");
		String indent = cl.propString("indent");
		System.out.println(
				String.format("确认要删除网盘 %s（%s） 吗？[y(yes),n]", disk.name(),disk.info().alias()));
		boolean isOk = ConsoleEditor.confirmConsole("y", "n", indent,
				ConsoleEditor.newReader());
		if (isOk) {
			disk.delete();
		}
	}

	@Override
	public String cmd() {
		return "drop";
	}

	@Override
	public String cmdDesc() {
		return "删除当前网盘。无参数";
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
