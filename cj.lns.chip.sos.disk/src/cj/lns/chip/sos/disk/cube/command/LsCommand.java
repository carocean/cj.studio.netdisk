package cj.lns.chip.sos.disk.cube.command;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.cli.Options;
import org.bson.Document;

import cj.lns.chip.sos.cube.framework.Coordinate;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "lsTupleCommand")
public class LsCommand extends Command {

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "cubeConsole")
	Console cubeConsole;

	public void doCommand(CmdLine cl) throws IOException {
		ICube cube = (ICube) cl.prop("cube");
		// FileSystem fs = cube.fileSystem();
		// CommandLine line = cl.line();
		String indent = cl.propString("indent");
		// @SuppressWarnings("unchecked")
		// List<String> args = line.getArgList();
		// if (args.isEmpty()) {
		// throw new EcmException("需要参数，格式：tuple 某指令");
		// }
		// String op = args.get(0).trim();// 指令
		List<Coordinate> tuples = cube.tupleCoordinate();
		Collections.sort(tuples,new Comparator<Coordinate>(){
			@Override
			public int compare(Coordinate o1, Coordinate o2) {
				// TODO Auto-generated method stub
				return ((String)o1.value()).compareTo((String)o2.value());
			}
		});
		System.out.println(String.format("%s元组：", indent));
		System.out.println(String.format("%s\t元组名\t个数\t大小\t已用空间\t索引占用：", indent));
		for (Coordinate c : tuples) {
			Document doc = cube.tupleStats((String) c.value());
			System.out.println(String.format("%s\t%s\t%s\t%s\t%s\t%s", indent,
					c.value(), doc.get("count"), doc.get("size"),
					doc.get("storageSize"),doc.get("totalIndexSize")));
		}
	}

	@Override
	public String cmd() {
		return "ls";
	}

	@Override
	public String cmdDesc() {
		return "用于查看存储空间中的元组列表。例：ls";
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
