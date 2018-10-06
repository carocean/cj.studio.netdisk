package cj.lns.chip.sos.disk.usf.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.INetDisk;
import cj.lns.chip.sos.disk.NetDisk;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "OpenDiskCommand")
public class OpenDiskCommand extends Command {
	@CjServiceInvertInjection
	@CjServiceRef(refByName = "usfConsole")
	Console usfconsole;
	@CjServiceRef(refByName = "diskConsole")
	Console diskconsole;

	public void doCommand(CmdLine cl) throws IOException {
		MongoClient client = (MongoClient) cl.prop("client");
		CommandLine line = cl.line();
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		String indent = (String) cl.prop("indent");
		if (args.isEmpty()) {
			System.out.println(String.format("%s错误：未指定网盘名", indent));
			return;
		}
		String diskname = args.get(0);
		if (!line.hasOption("u")) {
			System.out.println(String.format("%s缺少参数：-u ", indent));
			return;
		}
		if (!line.hasOption("p")) {
			System.out.println(String.format("%s缺少参数：-p ", indent));
			return;
		}
		
		INetDisk disk = NetDisk.open(client, diskname, line.getOptionValue("u"),
				line.getOptionValue("p"));
		System.out.println(String.format("%s网盘：%s", indent,disk.name()));
		System.out.println(String.format("%s\t网盘别名：\t%s", indent,disk.info().alias()));
		System.out.println(String.format("%s\t网盘大小：\t%s", indent,disk.useSpace()));
		System.out.println(String.format("%s\t网盘已用：\t%s", indent,disk.dataSize()));
		System.out.println(String.format("%s\t存储空间数：\t%s", indent,disk.cubeCount()));
		
		diskconsole.monitor(client, disk, cl.prop("indent"));
	}

	@Override
	public String cmd() {
		return "open";
	}

	@Override
	public String cmdDesc() {
		// TODO Auto-generated method stub
		return "打开网盘";
	}

	@Override
	public Options options() {
		Options options = new Options();
		// Option name = new Option("n", "name",true, "网盘名");
		// options.addOption(name);
		Option u = new Option("u", "user", true, "用户名");
		options.addOption(u);
		Option p = new Option("p", "password", true, "密码");
		options.addOption(p);
		return options;
	}
}
