package cj.lns.chip.sos.disk.cube.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCursor;

import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.ConsoleEditor;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.ultimate.util.StringUtil;

@CjService(name = "MrTupleCommand")
public class MrCommand extends Command {

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
			throw new EcmException("需要参数，格式：mr tupleName");
		}
		String tupleName = args.get(0).trim();// 指令
		StringBuffer sb = new StringBuffer();
		System.out
				.println(String.format("%s输入aggregate管道语法（以!q号结输输入):", indent));
		String split = "\r\n";
		ConsoleEditor.readConsole(indent, split, ConsoleEditor.newReader(), sb);
		String piplelines = sb.toString().trim();
		if (StringUtil.isEmpty(piplelines) || "\r\n".equals(piplelines)) {
			return;
		}
		String arr[] = piplelines.split(split);
		List<Bson> list = new ArrayList<>();
		for (String str : arr) {
			Document doc = Document.parse(str);
			list.add(doc);
		}
		AggregateIterable<Document> result = cube.aggregate(tupleName, list);
		printDocs(cube, indent, result);
	}

	private void printDocs(ICube cube, String indent,
			AggregateIterable<Document> result) {
		MongoCursor<Document> it = result.iterator();
		while (it.hasNext()) {
			Document doc = it.next();
			System.out.println(String.format("%s%s", indent, doc.toJson()));
		}
	}

	@Override
	public String cmd() {
		return "mr";
	}

	@Override
	public String cmdDesc() {
		return "按aggregate语法进行统计运算。例：mr inbox 然后在输入框中输入管道。\r\n注意：在统计元组字段时，必须在字段前加:tuple关键字，否则可能得不到结果";
	}

	@Override
	public Options options() {
		Options options = new Options();
		// Option t = new Option("c", "count", false, "要求输入count语句");
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
