package cj.lns.chip.sos.disk.usf.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.mongodb.MongoClient;

import cj.lns.chip.sos.cube.framework.CubeConfig;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.DiskInfo;
import cj.lns.chip.sos.disk.NetDisk;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;
import cj.studio.ecm.resource.IResource;

@CjService(name = "createDiskCommand")
public class CreateDiskCommand extends Command {
	@CjServiceInvertInjection
	@CjServiceRef(refByName = "usfConsole")
	Console usfconsole;

	@Override
	public void doCommand(CmdLine cl) {

		MongoClient client = (MongoClient) cl.prop("client");
		CommandLine line = cl.line();
		@SuppressWarnings("unchecked")
		List<String> args = line.getArgList();
		String indent = (String) cl.prop("indent");
		if (args.isEmpty()) {
			System.out.println(String.format("%s错误：未指定要创建的网盘名", indent));
			return;
		}
		if (!line.hasOption("u")) {
			System.out.println(String.format("%s缺少参数：-u ", indent));
			return;
		}
		if (!line.hasOption("p")) {
			System.out.println(String.format("%s缺少参数：-p ", indent));
			return;
		}
		if (!line.hasOption("alias")) {
			System.out.println(String.format("%s缺少参数：-alias ", indent));
			return;
		}
		String diskname = args.get(0).trim();
		String userName = line.getOptionValue("u").trim();
		String password = line.getOptionValue("p").trim();
		String alias = line.getOptionValue("alias").trim();
		if (NetDisk.existsDisk(client, diskname)) {
			System.out.println(String.format("%s网盘已存在：%s", indent, diskname));
			return;
		}
		InputStreamReader input = new InputStreamReader(System.in);
		BufferedReader read = new BufferedReader(input);
		try {
			System.out.println(String.format("%s请再次输入密码:", indent));
			String check = read.readLine();
			if (!password.equals(check)) {
				System.out.println("密码不匹配");
				return;
			}
		} catch (Exception e) {
			throw new EcmException(e);
		}
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
		String home = String.format("%s 的主存储空间", alias);
		conf.alias(home);
		conf.setDimFile(dimsFile);
		conf.setCoordinateFile(coordsFile);

		DiskInfo info = new DiskInfo(alias, conf);
		NetDisk.create(client, diskname, userName, password, info);
	}

	@Override
	public String cmd() {
		return "create";
	}

	@Override
	public String cmdDesc() {
		// TODO Auto-generated method stub
		return "创建网盘。例：\r\ncreate cctv -alias 中央电视台网盘 -u carocean -p 11";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option alias = new Option("alias", "alias", true, "网盘别名，中文名");
		options.addOption(alias);
		Option u = new Option("u", "user", true, "用户名");
		options.addOption(u);
		Option p = new Option("p", "password", true, "密码");
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
