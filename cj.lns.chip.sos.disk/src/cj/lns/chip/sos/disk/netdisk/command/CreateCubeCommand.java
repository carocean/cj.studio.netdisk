package cj.lns.chip.sos.disk.netdisk.command;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.CubeConfig;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.INetDisk;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.resource.IResource;
@CjService(name="createCubeCommand")
public class CreateCubeCommand extends Command{

	@CjServiceInvertInjection
	@CjServiceRef(refByName = "diskConsole")
	Console diskConsole;
	@CjServiceRef(refByName="cubeConsole")
	Console cubeConsole;
	public void doCommand(CmdLine cl) throws IOException {
		INetDisk disk=(INetDisk)cl.prop("disk");
		CommandLine line=cl.line();
		String indent = (String) cl.prop("indent");
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		if (args.isEmpty()){
			System.out.println(String.format("%s错误：未指定要打开的存储空间名", indent));
			return;// 回到主目录下
		}
		
		if (!line.hasOption("alias")) {
			System.out.println(String.format("%s缺少参数：-alias ", indent));
			return;
		}
		String alias=line.getOptionValue("alias");
		if (!line.hasOption("c")) {
			System.out.println(String.format("%s缺少参数：-c ", indent));
			return;
		}
		double capacity=Double.valueOf(line.getOptionValue("c"));
		
		String cubename = args.get(0).trim();
		IResource res = (IResource) this.getClass().getClassLoader();
		String appDir = new File(res.getResourcefile()).getParent();
		// String appDir = System.getProperty("user.dir");
		String dimsFile = String.format("%s/conf/system-dims.bson", appDir);
		if (!new File(dimsFile).exists()) {
			System.out.println(
					String.format("%s错误：维度定义文件不存在：%s ", indent, dimsFile));
			return;
		}
		String coordsFile = String.format("%s/conf/system-coordinates.bson",
				appDir);
		if (!new File(coordsFile).exists()) {
			System.out.println(
					String.format("%s警告：坐标定义文件不存在：%s ", indent, coordsFile));
		}
		CubeConfig conf = new CubeConfig();
		double chunkColThresholdCount = 40000;
		if (line.hasOption("fbc")) {
			chunkColThresholdCount = Double
					.parseDouble(line.getOptionValue("fbc"));
		}
		double chunkColThresholdSize = 8D * 1024 * 1024 * 1024;
		if (line.hasOption("fbs")) {
			chunkColThresholdSize = Double
					.parseDouble(line.getOptionValue("fbs"));
		}
		conf.setChunkColThresholdCount(chunkColThresholdCount);
		conf.setChunkColThresholdSize(chunkColThresholdSize);
		conf.alias(alias);
		if(line.hasOption("desc")){
			conf.setDesc(line.getOptionValue("desc"));
		}
		conf.setCapacity(capacity);
		conf.setDimFile(dimsFile);
		conf.setCoordinateFile(coordsFile);
		disk.createCube(cubename,conf);
	}

	@Override
	public String cmd() {
		return "create";
	}
	@Override
	public String cmdDesc() {
		return "创建存储空间。例：create xxx";
	}
	@Override
	public Options options() {
		Options options = new Options();
		Option alias = new Option("alias", "alias",true, "别名");
		options.addOption(alias);
		Option c = new Option("c", "capacity",true, "存储空间容量，－1表示不受限。单位是字节");
		options.addOption(c);
		Option p = new Option("desc", "description",true, "说明");
		options.addOption(p);
		Option fbc = new Option("fbc", "fbcount", true,
				"文件系统块集合产生以记录数为阀值，默认为4万条产生新的块集合，-1表示永远使用一个块。此值可在system_fs_assigner集合中修改");
		options.addOption(fbc);
		Option fbs = new Option("fbs", "fbsize", true,
				"文件系统 块集合产生以集合数据大小为阀值，默认8G，且与参数-fbc均达到阀值才生成新块集合。此值可在system_fs_assigner集合中修改");
		options.addOption(fbs);
		return options;
	}
}
