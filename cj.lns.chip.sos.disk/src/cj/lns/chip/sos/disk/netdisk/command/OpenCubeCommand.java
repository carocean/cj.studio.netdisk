package cj.lns.chip.sos.disk.netdisk.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.INetDisk;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "openCubeCommand")
public class OpenCubeCommand extends Command {

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "diskConsole")
	Console diskConsole;
	@CjServiceRef(refByName = "cubeConsole")
	Console cubeConsole;

	public void doCommand(CmdLine cl) throws IOException {
		INetDisk disk = (INetDisk) cl.prop("disk");
		CommandLine line = cl.line();
		String indent = (String) cl.prop("indent");
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty()) {
			printCubeInfo(disk, disk.home(), true, indent);
			cubeConsole.monitor((MongoClient) cl.prop("client"), disk.home(),
					indent);
			return;
		}
		String cubename = args.get(0).trim();
		if("home".equalsIgnoreCase(cubename)){
			printCubeInfo(disk, disk.home(), true, indent);
			cubeConsole.monitor((MongoClient) cl.prop("client"), disk.home(),
					indent);
			return;
		}
		ICube cube = disk.cube(cubename);
		printCubeInfo(disk, cube, false, indent);
		cubeConsole.monitor((MongoClient) cl.prop("client"), cube, indent);
	}

	private void printCubeInfo(INetDisk disk, ICube cube, boolean isHome,
			String indent) {
		if (isHome) {
			System.out.println(String.format("%s存储空间：home", indent));
		} else {
			System.out.println(String.format("%s存储空间：%s", indent,
					disk.getCubeName(cube.name())));
		}
		System.out.println(String.format("%s\t标识：\t%s", indent, cube.name()));
		System.out.println(String.format("%s\t别名：\t%s", indent,
				cube.config().alias() == null ? "-" : cube.config().alias()));
		System.out.println(String.format("%s\t空间容量：\t%s", indent,
				cube.config().getCapacity() == -1 ? "不限"
						: cube.config().getCapacity()));
		System.out.println(
				String.format("%s\t空间占用：\t%s", indent, cube.usedSpace()));
		System.out.println(
				String.format("%s\t数据大小：\t%s", indent, cube.dataSize()));
		System.out.println(String.format("%s\t用途：\t%s", indent,
				cube.config().getDesc() == null ? "-"
						: cube.config().getDesc()));
	}

	@Override
	public String cmd() {
		return "open";
	}

	@Override
	public String cmdDesc() {
		return "打开存储空间。例：open xxxx，如果为空表示进入主存储空间";
	}

	@Override
	public Options options() {
		Options options = new Options();
		// Option name = new Option("n", "name",true, "cube名");
		// options.addOption(name);
		// Option u = new Option("u", "user",true, "用户名");
		// options.addOption(u);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
