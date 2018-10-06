package cj.lns.chip.sos.disk.netdisk.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.ConsoleEditor;
import cj.lns.chip.sos.disk.INetDisk;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "delCubeCommand")
public class DeleteCubeCommand extends Command {

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "diskConsole")
	Console diskConsole;

	public void doCommand(CmdLine cl) throws IOException {
		INetDisk disk = (INetDisk) cl.prop("disk");
		String indent = cl.propString("indent");
		CommandLine line = cl.line();
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty()) {
			throw new EcmException("需要参数，格式：del 存储空间名");
		}
		String cubeName = args.get(0);
		System.out
				.println(String.format("确认要删除存储空间 %s 吗？[y(yes),n]", cubeName));
		boolean isOk = ConsoleEditor.confirmConsole("y", "n", indent,
				ConsoleEditor.newReader());
		if (isOk) {
			disk.deleteCube(cubeName);
		}
	}

	@Override
	public String cmd() {
		return "del";
	}

	@Override
	public String cmdDesc() {
		return "删除存储空间。用法：del xxxx";
	}

	@Override
	public Options options() {
		Options options = new Options();
		// Option t = new Option("n", "name", true, "元组名");
		// options.addOption(t);
		// Option u = new Option("e", "editor", false, "开启编辑框接受输入");
		// options.addOption(u);
		// Option set = new Option("set", "setCoordinate", false, "是否设置坐标");
		// options.addOption(set);
		// Option cdm = new Option("r", "recurse", false,
		// "此参数的每一次将统计其下各级的文件数，并返回每级总数，无此参数则每级仅统计本级直接包含的文件。");
		// options.addOption(cdm);
		// Option tp = new Option("tp", "tuple", true,
		// "对文件进行多维元维查询，支持多个坐标查询，格式:{'createDate':'2015/10/23','fileType':'doc','dir':'/我的文件/电影'}");
		// options.addOption(tp);
		// Option cdn = new Option("n", "none", false, "不显示文件数，只显示统计。");
		// options.addOption(cdn);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
