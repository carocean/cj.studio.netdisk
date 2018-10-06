package cj.lns.chip.sos.disk.cube.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.bson.Document;
import org.bson.conversions.Bson;

import cj.lns.chip.sos.cube.framework.Coordinate;
import cj.lns.chip.sos.cube.framework.Dimension;
import cj.lns.chip.sos.cube.framework.Hierarcky;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.cube.framework.IDocument;
import cj.lns.chip.sos.cube.framework.IQuery;
import cj.lns.chip.sos.cube.framework.Level;
import cj.lns.chip.sos.cube.framework.Property;
import cj.lns.chip.sos.cube.framework.TupleDocument;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.ConsoleEditor;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.ultimate.gson2.com.google.gson.Gson;
import cj.ultimate.gson2.com.google.gson.reflect.TypeToken;
import cj.ultimate.util.StringUtil;

@CjService(name = "tupleCommand")
public class TupleCommand extends Command {

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
		List<String> table = new ArrayList<>();
		for (String e : args) {
			if (StringUtil.isEmpty(e))
				continue;
			table.add(e);
		}
		if (table.isEmpty()) {
			throw new EcmException("需要参数，格式：tuple tupleName 某指令");
		}
		if (table.size() < 2) {
			throw new EcmException("缺少指令参数，格式：tuple tupleName 某指令");
		}
		String tupleName = table.get(0).trim();// 指令
		String op = table.get(1).trim();

		InputStreamReader input = new InputStreamReader(System.in);
		BufferedReader read = new BufferedReader(input);
		StringBuffer sb =null;
		switch (op) {
		case "index":
			if (!line.hasOption("e")) {
				throw new EcmException("缺少-e参数");
			}
			sb = new StringBuffer();
			System.out.println(String.format("%s输入内容（以!q号结输输入):", indent));
			ConsoleEditor.readConsole(indent, read, sb);
			String jsonkeys = sb.toString().trim();
			if (!StringUtil.isEmpty(jsonkeys)) {
				Bson keys=Document.parse(jsonkeys);
				cube.createIndex(tupleName, keys);
			}
			break;
		case "save":
			if (!line.hasOption("e")) {
				throw new EcmException("缺少-e参数");
			}
			Map<String, Coordinate> coords = null;
			if (line.hasOption("set")) {
				sb = new StringBuffer();
				System.out.println(String.format(
						"%s添加坐标，如果没有直接输入!q结束输入，格式：\t\t\r\n{'dimName':'/xxx/xxx/xxx','dimName2':'/wwww/www/ww'}",
						indent));
				ConsoleEditor.readConsole(indent, read, sb);
				String json = sb.toString().trim();
				if (!StringUtil.isEmpty(json)) {
					coords = cube.parseCoordinate(json);
				}
			}
			sb = new StringBuffer();
			System.out.println(String.format("%s输入内容（以!q号结输输入):", indent));
			ConsoleEditor.readConsole(indent, read, sb);
			String json = sb.toString().trim();
			if (!StringUtil.isEmpty(json)) {
				Map<String, Object> map = new Gson().fromJson(sb.toString(),
						new TypeToken<HashMap<String, Object>>() {
						}.getType());

				TupleDocument<Map<String, Object>> doc = new TupleDocument<Map<String, Object>>(
						map);
				if (coords != null) {
					Set<String> coorSet = coords.keySet();
					for (String dimName : coorSet) {
						doc.addCoordinate(dimName,
								coords.get(dimName).goHead());
					}
				}
				cube.saveDoc(tupleName, doc);
			}
			break;
		case "ls":// 如果无参数则显示所有记录，可指定起始位置
			String cql = "select {'tuple':'*'}";
			if (line.hasOption("skip")) {
				cql = String.format("%s.skip(%s)", cql,
						line.getOptionValue("skip"));
			}
			if (line.hasOption("limit")) {
				cql = String.format("%s.limit(%s)", cql,
						line.getOptionValue("limit"));
			}
			if (line.hasOption("where")) {
				sb = new StringBuffer();
				System.out.println(String.format(
						"%s输入查询条件（json对象格式，以!q号结输输入):\r\n\t\t例：{'tuple.name':'cj'}",
						indent));
				ConsoleEditor.readConsole(indent, read, sb);
				String wh = sb.toString().trim();
				if (StringUtil.isEmpty(wh)) {
					wh = "{}";
				}
				cql = String.format(" %s from tuple %s %s where %s", cql,
						tupleName, HashMap.class.getName(), wh);
			} else {
				cql = String.format(" %s from tuple %s %s where {}", cql,
						tupleName, HashMap.class.getName());
			}
			queryAndPrint(cube, indent, cql);
			break;
		case "del":
			if (!line.hasOption("id")) {
				throw new EcmException("缺少-id参数");
			}
			String id = line.getOptionValue("id").trim();
			if (line.hasOption("where")) {
				sb = new StringBuffer();
				System.out.println(String.format(
						"%s输入查询条件（json对象格式，以!q号结输输入):\r\n\t\t例：{'tuple.name':'cj'}",
						indent));
				ConsoleEditor.readConsole(indent, read, sb);
				String wh = sb.toString().trim();
				if (StringUtil.isEmpty(wh)) {
					wh = "{}";
				}
				cube.deleteDocs(tupleName, wh);
			} else {
				cube.deleteDoc(tupleName, id);
			}
			break;
		case "cube":
			if (!line.hasOption("where")) {
				throw new EcmException("缺少-where参数");
			}
			sb = new StringBuffer();
			System.out.println(String.format(
					"%s按坐标系查询元组数据格式：{'date':'/2015/3/12','type':'html'}",
					indent));
			ConsoleEditor.readConsole(indent, read, sb);
			String cds = sb.toString().trim();
			if (StringUtil.isEmpty(cds)) {
				cds = "{}";
			}
			Map<String, Coordinate> coordmap = cube.parseCoordinate(cds);
			@SuppressWarnings("rawtypes")
			List<IDocument<HashMap>> docslist = cube.listTuplesByCoordinate(
					tupleName, HashMap.class, coordmap, line.hasOption("r"));
			printDocs(cube, indent, docslist);
			break;
		case "drop":
			System.out.println(
					String.format("%s是否删除元组：%s？是输入y,否输入n", indent, tupleName));
			boolean isOk = ConsoleEditor.confirmConsole("y", "n", indent,
					ConsoleEditor.newReader());
			if (isOk) {
				cube.dropTuple(tupleName);
			}
			break;
		case "coord":
			if (line.hasOption("dim")) {
				lsCoord(indent, line, tupleName, cube,
						line.getOptionValue("dim"));
			} else {
				lsCoords(indent, line, tupleName, cube);
			}
			break;
		case "update":
			if (!line.hasOption("id")) {
				throw new EcmException("缺少-id参数");
			}
			id = line.getOptionValue("id").trim();
			if (!line.hasOption("e")) {
				throw new EcmException("缺少-e参数");
			}
			coords = null;
			if (line.hasOption("set")) {
				sb = new StringBuffer();
				System.out.println(String.format(
						"%s添加坐标，如果没有直接输入!q结束输入，格式：\t\t\r\n{'dimName':'/xxx/xxx/xxx','dimName2':'/wwww/www/ww'}",
						indent));
				ConsoleEditor.readConsole(indent, read, sb);
				json = sb.toString().trim();
				if (!StringUtil.isEmpty(json)) {
					coords = cube.parseCoordinate(json);
				}
			}
			sb = new StringBuffer();
			System.out.println(String.format("%s输入内容（以!q号结输输入):", indent));
			ConsoleEditor.readConsole(indent, read, sb);
			json = sb.toString().trim();
			if (!StringUtil.isEmpty(json)) {
				Map<String, Object> map = new Gson().fromJson(sb.toString(),
						new TypeToken<HashMap<String, Object>>() {
						}.getType());

				TupleDocument<Map<String, Object>> doc = new TupleDocument<Map<String, Object>>(
						map);
				if (coords != null) {
					Set<String> coorSet = coords.keySet();
					for (String dimName : coorSet) {
						doc.addCoordinate(dimName, coords.get(dimName));
					}
				}
				cube.updateDoc(tupleName, id, doc);
			}

			break;
		default:
			throw new EcmException("不支持的操作");
		}

	}

	private void lsCoords(String indent, CommandLine line, String tupleName,
			ICube cube) {
		List<String> dims = cube.enumDimension();
		System.out.println(String.format("%s共有%s个维度", indent, dims.size()));
		System.out.println(
				"-------------------------------------------------------");
		for (String dimName : dims) {
			lsCoord(indent, line, tupleName, cube, dimName);
		}
	}

	private void lsCoord(String indent, CommandLine line, String tupleName,
			ICube cube, String dimName) {
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

		List<Coordinate> members = cube.rootCoordinates(tupleName, dimName);
		printCoords(members, String.format("%s\t\t\t", indent), tupleName, cube,
				dim, line);
		System.out.println(
				"-------------------------------------------------------");
	}

	@SuppressWarnings("rawtypes")
	private void printCoords(List<Coordinate> members, String indent,
			String tupleName, ICube cube, Dimension dim, CommandLine line) {
		for (Coordinate coord : members) {
			List<IDocument<HashMap>> map = cube.listTuplesByCoordinate(
					tupleName, HashMap.class, dim.getName(), coord,
					line.hasOption("r"));
			System.out.println(String.format("%s%s  %s：%s", indent,
					coord.value(), tupleName, map.size()));
			if (!line.hasOption("off")) {
				String cntIndent = String.format("%s     ", indent);
				System.out.println(String.format(
						"%s^-------------------------------------@",
						cntIndent));
				printDocs(cube, cntIndent, map);
				System.out.println(String.format(
						"%s@-------------------------------------$",
						cntIndent));
			}
			List<Coordinate> childs = cube.childCoordinates(tupleName,
					dim.getName(), coord);
			// if (childs.isEmpty()) {
			// break;
			// }
			printCoords(childs, String.format("%s\t", indent), tupleName, cube,
					dim, line);

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

	private void queryAndPrint(ICube cube, String indent, String cql) {
		IQuery<Map<String, Object>> q = cube.createQuery(cql);
		List<IDocument<Map<String, Object>>> result = q.getResultList();
		for (IDocument<Map<String, Object>> t : result) {
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
		System.out.println(String.format("%s共%s条", indent, result.size()));
	}

	@Override
	public String cmd() {
		return "tuple";
	}

	@Override
	public String cmdDesc() {
		return "文档等结构化数据指令。例：tuple tupleName ls -limit 3\r\n"
				+ "\ttuple tupleName ls 列出元组数据.可选参数：-skip n -limit n -where\r\n"
				+ "\ttuple tupleName cube 按坐标系查询或统计元组。格式：{'date':'/2015/3/12','type':'html'}.必选参数：-where 可选-r \r\n"
				+ "\ttuple tupleName coord 列出元组中的坐标并按坐标列出元组。可选参数：-dim -off -r\r\n"
				+ "\ttuple tupleName save 保存元组数据。必选参数：-e 可选：-set\r\n"
				+ "\ttuple tupleName update 更新元组数据.必选参数：-id -e 可选：-set\r\n"
				+ "\ttuple tupleName drop 删除元组及其所有数据\r\n"
				+ "\ttuple tupleName del 删除元组数据，必选参数：-id，可选-where"
				+ "\ttuple tupleName index 创建索引，必选参数 -e";
	}

	@Override
	public Options options() {
		Options options = new Options();

		Option id = new Option("id", "id", true, "元组的标识，一般用于删除或更新");
		options.addOption(id);
		Option u = new Option("e", "editor", false, "开启编辑框接受输入");
		options.addOption(u);
		Option where = new Option("where", "where", false,
				"输入查询条件，bson格式，必须以{}");
		options.addOption(where);
		Option set = new Option("set", "setCoordinate", false, "是否设置坐标");
		options.addOption(set);
		Option pos = new Option("skip", "skip", true, "跳过的记录数");
		options.addOption(pos);
		Option limit = new Option("limit", "limit", true, "限制返回行数");
		options.addOption(limit);
		Option dim = new Option("dim", "dimension", true, "限制返回行数");
		options.addOption(dim);
		Option off = new Option("off", "off", false,
				"coord指令：关闭级别的显示及不显示元组数据只统计");
		options.addOption(off);
		Option cdm = new Option("r", "recurse", false,
				"此参数的每一次将统计其下各级的文件数，并返回每级总数，无此参数则每级仅统计本级直接包含的文件。");
		options.addOption(cdm);
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
