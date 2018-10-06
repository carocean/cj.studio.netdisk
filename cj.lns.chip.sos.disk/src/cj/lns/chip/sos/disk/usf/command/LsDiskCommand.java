package cj.lns.chip.sos.disk.usf.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.Options;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.NetDisk;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
@CjService(name="lsDiskCommand")
public class LsDiskCommand extends Command{

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "usfConsole")
	Console usfconsole;
	
	public void doCommand(CmdLine cl) throws IOException {
		MongoClient client = (MongoClient) cl.prop("client");
		List<String> disks=NetDisk.enumDisk(client);
		for(String name:disks){
			System.out.println(String.format("%s%s", cl.prop("indent"),name));
		}
	}
	@Override
	public String cmdDesc() {
		// TODO Auto-generated method stub
		return "列出网盘";
	}
	@Override
	public String cmd() {
		return "ls";
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
