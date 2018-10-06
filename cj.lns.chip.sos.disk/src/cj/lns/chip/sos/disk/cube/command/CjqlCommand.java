package cj.lns.chip.sos.disk.cube.command;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.Dimension;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.ConsoleEditor;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.ultimate.gson2.com.google.gson.Gson;

@CjService(name = "cjqlTupleCommand")
public class CjqlCommand extends Command {

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "cubeConsole")
	Console cubeConsole;

	public void doCommand(CmdLine cl) throws IOException {
		ICube cube = (ICube) cl.prop("cube");
		// FileSystem fs = cube.fileSystem();
		CommandLine line = cl.line();
		String indent = cl.propString("indent");
		// @SuppressWarnings("unchecked")
		// List<String> args = line.getArgList();
		// if (args.isEmpty()) {
		// throw new EcmException("需要参数，格式：tuple 某指令");
		// }
		// String op = args.get(0).trim();// 指令
		StringBuffer sb = new StringBuffer();
		System.out.println(String.format("%s输入cjql语句（以!q号结输输入):", indent));
		ConsoleEditor.readConsole(indent, ConsoleEditor.newReader(), sb);
		String cjql = sb.toString().trim();
		@SuppressWarnings("rawtypes")
		IQuery<HashMap> q = null;

		if (line.hasOption("c")) {
			q = cube.count(cjql);
			System.out.println(String.format("%s结果：%s", indent, q.count()));
		} else {
			q = cube.createQuery(cjql);
			@SuppressWarnings("rawtypes")
			List<IDocument<HashMap>> list = q.getResultList();
			printDocs(cube, indent, list);
		}

	}

	@SuppressWarnings("rawtypes")
	private void printDocs(ICube cube, String indent,
			List<IDocument<HashMap>> coordlist) {
		for (IDocument<HashMap> t : coordlist) {
			System.out.println(String.format("%s标识:%s", indent, t.docid()));
			System.out.println(String.format("%s\t坐标:", indent));
			String[] arr = t.enumCoordinate();
			for (String dimName : arr) {
				Dimension dim = cube.dimension(dimName);
				String path = t.coordinate(dimName).toPath();
				System.out.println(String.format("%s\t\t%s %s:%s", indent,
						(dim == null ? "" : dim.getAlias()), dimName, path));
			}
			System.out.println(String.format("%s\t内容:", indent));
			System.out.println(String.format("%s\t\t%s", indent,
					new Gson().toJson(t.tuple())));
		}
	}

	@Override
	public String cmd() {
		return "cjql";
	}

	@Override
	public String cmdDesc() {
		return "按cjql语法自由查询。例：select {'tuple.name':'1'}.limit(10) from {}";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option t = new Option("c", "count", false, "要求输入count语句");
		options.addOption(t);
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
