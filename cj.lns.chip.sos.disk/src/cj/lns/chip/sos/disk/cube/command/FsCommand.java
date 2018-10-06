package cj.lns.chip.sos.disk.cube.command;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.cube.framework.Coordinate;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.ICube;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.ConsoleEditor;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "fsCommand")
public class FsCommand extends Command {

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "cubeConsole")
	Console cubeConsole;
	@CjServiceRef(refByName = "fsConsole")
	Console fsConsole;

	public void doCommand(CmdLine cl) throws IOException {
		ICube cube = (ICube) cl.prop("cube");
		FileSystem fs = cube.fileSystem();
		CommandLine line = cl.line();
		String indent = cl.propString("indent");

		if (line.hasOption("ft")) {
			List<String> types = fs.enumFileType();
			for (String t : types) {
				List<FileInfo> files = fs.listFilesByType(t);
				System.out.println(
						String.format("%s%s\t文件数：%s", indent, t, files.size()));
				if (!line.hasOption("n")) {
					for (FileInfo file : files) {
						System.out.println(String.format("%s\t%s %s", indent,
								file.spaceLength(), file.fullName()));
					}
				}
			}
		} else if (line.hasOption("ch")) {
			boolean lookall = line.hasOption("r");
			List<Coordinate> members = fs.coordinateRoot("system_fs_chunks");
			System.out.println(String.format("%s块集合", indent));
			String ind = String.format("%s\t", indent);
			for (Coordinate coord : members) {
				List<FileInfo> files = fs.listFilesByCoordinate(
						"system_fs_chunks", coord, lookall);
				printHead(ind, coord.value(), "col", files.size());
				if (!line.hasOption("n")) {
					printFiles(String.format("%s\t\t", ind), files);
				}
			}
		} else if (line.hasOption("cd")) {
			boolean lookall = line.hasOption("r");
			List<Coordinate> members = fs.coordinateRoot("createDate");
			System.out.println(String.format("%s创建日期", indent));
			String ind = String.format("%s\t", indent);
			for (Coordinate coord : members) {
				List<FileInfo> files = fs.listFilesByCoordinate("createDate",
						coord, lookall);
				printHead(ind, coord.value(), "年", files.size());
				// printFiles(ind, files);
				List<Coordinate> childs = fs.coordinateChilds("createDate",
						coord);
				for (Coordinate c : childs) {
					files = fs.listFilesByCoordinate("createDate", c, lookall);
					printHead(String.format("%s\t", ind), c.value(), "月",
							files.size());
					// printFiles(String.format("%s\t", ind), files);
					List<Coordinate> day = fs.coordinateChilds("createDate", c);
					for (Coordinate d : day) {
						files = fs.listFilesByCoordinate("createDate", d,
								lookall);
						printHead(String.format("%s\t\t", ind), d.value(), "日",
								files.size());
						if (!line.hasOption("n")) {
							printFiles(String.format("%s\t\t", ind), files);
						}
					}
				}

			}
		} else if (line.hasOption("tp")) {
			StringBuffer sb = new StringBuffer();
			InputStreamReader input = new InputStreamReader(System.in);
			BufferedReader read = new BufferedReader(input);
			System.out.println(
					String.format("%s输入坐标json格式，详请参见-tp参数（以!q号结输输入):", indent));
			ConsoleEditor.readConsole(indent, read, sb);
			String json = sb.toString().replace("\r\n", "").trim();
			Map<String, Coordinate> coords = cube.parseCoordinate(json);
			List<FileInfo> files = fs.listFilesByCoordinate(coords,
					line.hasOption("r"));
			System.out.println(String.format("%s坐标：%s", indent, json));

			System.out.println(String.format("%s文件数：%s", indent, files.size()));
			printFiles(String.format("%s\t", indent), files);
		} else if (line.hasOption("l")) {
			System.out.println(
					String.format("%s总目录数：%s", indent, fs.dirsTotal()));
			System.out.println(
					String.format("%s总文件数：%s", indent, fs.filesTotal()));
		} else {
			fsConsole.monitor((MongoClient) cl.prop("client"), fs, String
					.format("%s%s", cl.prop("indent"), cl.prop("indent")));
		}
	}

	private void printHead(String indent, Object value, String type,
			long count) {
		System.out.println(
				String.format("%s%s %s 文件数：%s", indent, value, type, count));
	}

	private void printFiles(String indent, List<FileInfo> files) {
		for (FileInfo file : files) {
			System.out
					.println(String.format("%s\t%s", indent, file.fullName()));
		}
	}

	@Override
	public String cmd() {
		return "fs";
	}

	@Override
	public String cmdDesc() {
		return "文件系统。无参数表示进入，参数用于多维度查看和统计文件";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option t = new Option("ft", "fileType", false, "按文件类型坐标查看文件系统");
		options.addOption(t);
		Option u = new Option("cd", "cdate", false, "按创建日期查看文件系统");
		options.addOption(u);
		Option ch = new Option("ch", "chunkCol", false, "按块集合查看文件系统");
		options.addOption(ch);
		Option l = new Option("l", "list", false, "列出文件系统的统计信息");
		options.addOption(l);
		Option cdm = new Option("r", "recurse", false,
				"此参数的每一次将统计其下各级的文件数，并返回每级总数，无此参数则每级仅统计本级直接包含的文件。");
		options.addOption(cdm);
		Option tp = new Option("tp", "tuple", false,
				"对文件进行多维元维查询，支持多个坐标查询，格式:{'createDate':'2015/10/23','fileType':'doc','dir':'/我的文件/电影'}");
		options.addOption(tp);
		Option cdn = new Option("n", "none", false, "不显示文件数，只显示统计。");
		options.addOption(cdn);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
