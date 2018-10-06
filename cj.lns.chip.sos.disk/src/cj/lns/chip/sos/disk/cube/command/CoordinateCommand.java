package cj.lns.chip.sos.disk.cube.command;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.Coordinate;
import cj.lns.chip.sos.cube.framework.Dimension;
import cj.lns.chip.sos.cube.framework.Hierarcky;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.Level;
import cj.lns.chip.sos.cube.framework.Property;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "coordinateCommand")
public class CoordinateCommand extends Command {

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "cubeConsole")
	Console cubeConsole;

	public void doCommand(CmdLine cl) throws IOException {
		ICube cube = (ICube) cl.prop("cube");
		// FileSystem fs = cube.fileSystem();
		CommandLine line = cl.line();
		String indent = cl.propString("indent");
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty()) {
			throw new EcmException("需要参数，格式：tuple 某指令");
		}
		String op = args.get(0).trim();// 指令
		switch (op) {
		case "ls":
			if (line.hasOption("n")) {
				lsCoord(indent, line, cube, line.getOptionValue("n"));
			} else {
				lsCoords(indent, line, cube);
			}

			break;
		case "export":
			if (!line.hasOption("f")) {
				throw new EcmException("缺少参数:-f");
			}
			export(indent, line.getOptionValue("f"), cube);
			break;
		case "import":
			imports(indent, line.getOptionValue("f"), cube);
			break;
		}
	}

	private void imports(String indent, String file, ICube cube) {
		try {
			cube.importCoordinates(file);
			System.out.println(String.format("%s成功导入", indent));
		} catch (Exception e) {
			throw e;
		}
	}

	private void export(String indent, String file, ICube cube) {
		cube.exportCoordinates(file);
	}

	private void lsCoord(String indent, CommandLine line, ICube cube,
			String dimName) {

		Dimension dim = cube.dimension(dimName);
		System.out.println(String.format("%s\t维度:%s", indent, dimName));
		System.out.println(String.format("%s\t\t标识:%s", indent, dim.id));
		System.out
				.println(String.format("%s\t\t别名:%s", indent, dim.getAlias()));
		System.out.println(String.format("%s\t\t描述:%s", indent,
				(dim.getDesc() == null ? "-" : dim.getDesc())));
		System.out.println(String.format("%s\t\t层级:", indent));
		if (!line.hasOption("off")) {
			Hierarcky hier = dim.hierarcky();
			Level head = hier.head();
			int i = 0;
			while (head != null) {
				Property p = head.property();
				String innerIndent = "";
				for (int j = 0; j < i + 1; j++) {
					innerIndent = String.format("%s\t", innerIndent);
				}
				innerIndent = String.format("%s%s", indent, innerIndent);
				System.out.println(
						String.format("%s\t\t级别序号:%s", innerIndent, i));
				System.out.println(String.format("%s\t\t级别名称:%s", innerIndent,
						p.getName()));
				System.out.println(String.format("%s\t\t级别别名:%s", innerIndent,
						p.getAlias()));
				System.out.println(String.format("%s\t\t数据类型:%s", innerIndent,
						p.getDataType()));
				head = head.nextLevel();
				i++;
			}
		}
		System.out.println(String.format("%s\t\t坐标:", indent));

		List<Coordinate> members = cube.rootCoordinates(dimName);
		printCoords(members, String.format("%s\t\t\t", indent), cube, dim);
		System.out.println(
				"-------------------------------------------------------");
	}

	private void printCoords(List<Coordinate> members, String indent,
			ICube cube, Dimension dim) {
		for (Coordinate coord : members) {
			System.out.println(String.format("%s%s", indent, coord.value()));
			List<Coordinate> childs = cube.childCoordinates(dim.getName(),
					coord);
			// if (childs.isEmpty()) {
			// break;
			// }
			printCoords(childs, String.format("%s\t", indent), cube, dim);

		}
	}

	private void lsCoords(String indent, CommandLine line, ICube cube) {
		List<String> dims = cube.enumDimension();
		System.out.println(String.format("%s共有%s个维度", indent, dims.size()));
		System.out.println(
				"-------------------------------------------------------");
		for (String dimName : dims) {
			lsCoord(indent, line, cube, dimName);
		}
	}

	@Override
	public String cmd() {
		return "coord";
	}

	@Override
	public String cmdDesc() {
		return "坐标定义.\r\ncoord ls 查看坐标，可选参数-n\r\ncoord export -f 导出坐标到本地文件\r\n\tcoord import -f 从本地文件导入坐标";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option t = new Option("n", "name", true, "维度名，关联ls");
		options.addOption(t);
		Option u = new Option("off", "off", false, "关闭级别的显示，关联ls");
		options.addOption(u);
		Option set = new Option("f", "file", true, "文件路径");
		options.addOption(set);
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
