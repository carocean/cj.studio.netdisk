package cj.lns.chip.sos.disk.netdisk.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.Options;

import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.INetDisk;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
@CjService(name="lsCubeCommand")
public class LsCubeCommand extends Command{

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "diskConsole")
	Console diskConsole;
	
	public void doCommand(CmdLine cl) throws IOException {
		INetDisk disk=(INetDisk)cl.prop("disk");
		List<String> names=disk.enumCube();
		System.out.println(String.format("共有%s个存储空间",disk.cubeCount()));
		System.out.println(String.format("%shome", cl.prop("indent")));
		for(String name:names){
			System.out.println(String.format("%s%s", cl.prop("indent"),name));
		}
	}

	@Override
	public String cmd() {
		return "ls";
	}
	@Override
	public String cmdDesc() {
		// TODO Auto-generated method stub
		return "列出存储空间";
	}
	@Override
	public Options options() {
		Options options = new Options();
//		Option name = new Option("n", "name",true, "网盘名");
//		options.addOption(name);
//		Option u = new Option("u", "user",true, "用户名");
//		options.addOption(u);
//		Option p = new Option("p", "password",true, "密码");
//		options.addOption(p);
		return options;
	}
}
