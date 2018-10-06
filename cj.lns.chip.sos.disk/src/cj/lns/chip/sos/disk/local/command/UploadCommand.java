package cj.lns.chip.sos.disk.local.command;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.IWriter;
import cj.lns.chip.sos.cube.framework.OpenMode;
import cj.lns.chip.sos.cube.framework.lock.FileLockException;
import cj.lns.chip.sos.disk.CancelHandle;
import cj.lns.chip.sos.disk.CmdLine;
import cj.lns.chip.sos.disk.Command;
import cj.lns.chip.sos.disk.Console;
import cj.lns.chip.sos.disk.ConsoleEditor;
import cj.studio.ecm.EcmException;
import cj.studio.ecm.annotation.CjService;
import cj.studio.ecm.annotation.CjServiceInvertInjection;
import cj.studio.ecm.annotation.CjServiceRef;

@CjService(name = "uploadLocalCommand")
public class UploadCommand extends Command {
	FileSystem fs;
	DirectoryInfo dir;
	@CjServiceInvertInjection
	@CjServiceRef(refByName = "localConsole")
	Console localConsole;
	@CjServiceRef(refByName = "cdLocalCommand")
	CdCommand local;

	public void doCommand(CmdLine cl) throws IOException {
		local.check();
		dir = (DirectoryInfo) cl.prop("dir");
		fs = (FileSystem) cl.prop("fs");
		String indent = (String) cl.prop("indent");
		CommandLine line = cl.line();
		if (line.hasOption("f")) {
			String file = line.getOptionValue("f").trim();
			if (!file.startsWith(File.separator)) {
				file = String.format("%s%s%s", local.dir.getPath(),
						File.separator, file);
			}
			File f = new File(file);
			if (!f.exists()) {
				System.out.println(String.format("%s文件不存在：%s", indent, file));
			} else {

				String remoteFileName = "";
				String remotePath = dir.path();
				if (!remotePath.endsWith("/")) {
					remotePath = String.format("%s/", remotePath);
				}
				remoteFileName = String.format("%s%s", remotePath, f.getName());
				CancelHandle cancel=null;
				try {
					System.out.println(String.format("%s输入q符并回撤则可撤消此任务", indent));
					cancel = ConsoleEditor.CancelTask();
					uploadFile(f, remoteFileName,cancel);
				} catch (Exception e) {
					throw new EcmException(e);
				}finally{
					if(cancel!=null){
						cancel.setCanceled(true);
					}
				}
			}
		}
		if (line.hasOption("d")) {
			String dir = line.getOptionValue("d").trim();
			if (".".equals(dir)) {
				dir = local.dir.getAbsolutePath();
			}
			if (!dir.startsWith(File.separator)) {
				dir = String.format("%s%s%s", local.dir.getPath(),
						File.separator, dir);
			}
			File f = new File(dir);
			if (!f.exists()) {
				System.out.println(String.format("%s路径不存在：%s", indent, dir));
			} else {
				CancelHandle cancel = null;
				try {
					System.out.println(String.format("%s输入q符并回撤则可撤消此任务", indent));
					cancel = ConsoleEditor.CancelTask();
					uploadFiles(f, f, line, cancel);
				} catch (Exception e) {
					throw new EcmException(e);
				} finally {
					if (cancel != null) {
						cancel.setCanceled(true);
					}
				}
			}
		}

	}

	private void uploadFiles(File root, File file, CommandLine line,
			CancelHandle cancel) throws FileLockException, IOException {
		if (cancel.isCanceled()) {
			return;
		}
		String path = file.getAbsolutePath();
		String remoteFileName = path.substring(root.getPath().length(),
				path.length());
		String remotePath = dir.path();
		if (!remotePath.endsWith("/")) {
			remotePath = String.format("%s/", remotePath);
		}
		remoteFileName = String.format("%s%s", remotePath, remoteFileName)
				.replace("//", "/");
		// 在此实现排除指定的一些目录
		// 在此实现排除文件的算法
		// 按名字进行排除，支持全名匹配，正则
		if (line.hasOption("ex")) {
			String name = file.getName();
			String rule = line.getOptionValue("ex");
			Pattern p = Pattern.compile(rule);
			Matcher m = p.matcher(name);
			if (m.matches()) {
				return;
			}
		}
		if (file.isDirectory()) {// 空目录不上传
			fs.dir(remoteFileName).mkdir(file.getName());
			File[] files = file.listFiles();
			for (File f : files) {
				uploadFiles(root, f, line, cancel);
			}
		} else {
			uploadFile(file, remoteFileName, cancel);
		}
	}

	private void uploadFile(File file, String remoteFileName,
			CancelHandle cancel) throws FileLockException, IOException {
		FileInfo fi = fs.openFile(remoteFileName, OpenMode.openOrNew);
		FileInputStream in = new FileInputStream(file);
		int read = 0;
		byte[] b = new byte[255 * 1024];
		IWriter writer = fi.writer(0);
		long pos = 0;
		long len = file.length();
		System.out.println(String.format("-- %s --%s\t",len, file.getAbsoluteFile()));
		System.out.print(String.format("     > %s", remoteFileName));
		System.out.print('\r');
		while ((read = in.read(b, 0, b.length)) > -1) {
			if (cancel.isCanceled()) {
				break;
			}
			writer.write(b, 0, read);
			pos += read;
			int per = (int) (((pos * 1.00F) / len) * 100);
			String str = String.format("%s%s", per, '%');
			for (int i = 0; i < str.length(); i++) {
				System.out.print('\b');
			}
			System.out.print(str);
		}
		System.out.print("\r\n");
		in.close();
		writer.close();
		if (cancel.isCanceled()) {
			fi.delete();
		}
	}

	@Override
	public String cmd() {
		return "upload";
	}

	@Override
	public String cmdDesc() {
		return "上传文件";
	}

	// public static void main(String...strings){
	// Pattern pattern=Pattern.compile("\\.git|.class");
	// System.out.println(pattern.matcher(".git").matches());
	// }
	@Override
	public Options options() {
		Options options = new Options();
		Option file = new Option("f", "file", true, "上传指定文件");
		options.addOption(file);
		Option dir = new Option("d", "dir", true, "上传指定目录及其下所有文件，如果上传当前目录，用.号");
		options.addOption(dir);
		Option c = new Option("ex", "exclude", true,
				"结合-d参数：排除目录或名字。按目录名和文件名进行排除，支持全名匹配，正则表达式，建议：\\.git|\\.gradle|.*\\.class，转义输入一条扛即可");
		options.addOption(c);
		return options;
	}
}
