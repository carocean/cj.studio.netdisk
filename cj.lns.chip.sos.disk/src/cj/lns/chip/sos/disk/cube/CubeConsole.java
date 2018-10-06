package cj.lns.chip.sos.disk.cube;

import java.io.IOException;
import java.util.Map;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.usf.UsfConsole;
import cj.studio.ecm.annotation.CjService;
@CjService(name="cubeConsole")
public class CubeConsole extends Console {
	ICube cube;
	@Override
	protected String prefix(MongoClient client, Object... target) {
		String indent=(String)target[1];
		String name="";
		if(cube.config().alias()!=null){
			name=cube.config().alias();
		}else{
			name=cube.name();
		}
		return UsfConsole.COLOR_CMDPREV + indent+name+ " >"
				+ UsfConsole.COLOR_CMDLINE;
	}
	@Override
	public void monitor(MongoClient client, Object... target)
			throws IOException {
		cube=(ICube)target[0];
		super.monitor(client, target);
	}
	@Override
	protected void beforDoCommand(Command cmd, CmdLine cl) {
		cl.prop("cube",cube);
		String indent=String.format("%s%s",cl.prop("indent"),cl.prop("indent"));
		cl.prop("indent",indent);
	}
	@Override
	protected void printMan(MongoClient client, Object[] target,Map<String, Command> cmds) {
		System.out.println(prefix(client, target)+"立方体指令集");
		super.printMan(client, target, cmds);
	}

	@Override
	protected boolean exit(String cmd) {
		if("close".equals(cmd)){
			cube.close();
			return true;
		}
		return false;
	}

}
