package cj.lns.chip.sos.disk.local.command;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import cj.lns.chip.sos.cube.framework.DirectoryInfo;
import cj.lns.chip.sos.cube.framework.FileInfo;
import cj.lns.chip.sos.cube.framework.FileSystem;
import cj.lns.chip.sos.cube.framework.IReader;
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

@CjService(name = "downloadLocalCommand")
public class DownloadCommand extends Command {
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
			String remotefile = line.getOptionValue("f").trim();
			String remotePath = dir.path();
			if (remotefile.startsWith("/")) {
				if (remotePath.endsWith("/")) {
					remotefile = String.format("%s%s",
							remotePath.substring(0, remotePath.length() - 1),
							remotefile);
				} else {
					remotefile = String.format("%s%s", remotePath, remotefile);
				}
			} else {
				if (remotePath.endsWith("/")) {
					remotefile = String.format("%s%s", remotePath, remotefile);
				} else {
					remotefile = String.format("%s/%s", remotePath, remotefile);
				}
			}

			if (!fs.existsFile(remotefile)) {
				System.out.println(
						String.format("%s远程文件不存在：%s", indent, remotefile));
			} else {
				CancelHandle cancel = null;
				try {
					FileInfo f = fs.openFile(remotefile, OpenMode.onlyOpen);
					String localFile = local.dir.getPath();
					localFile = String.format("%s%s%s", localFile,
							File.separator, f.name()).replace("//", "/");
					System.out.println(String.format("%s输入q符并回撤则可撤消此任务", indent));
					cancel = ConsoleEditor.CancelTask();
					downloadFile(f, new File(localFile),cancel);
				} catch (Exception e) {
					throw new EcmException(e);
				} finally {
					if (cancel != null) {
						cancel.setCanceled(true);
					}
				}
			}
		}
		if (line.hasOption("a")) {
			CancelHandle cancel=null;
			try {
				System.out.println(String.format("%s输入q符并回撤则可撤消此任务", indent));
				cancel = ConsoleEditor.CancelTask();
				downloadFiles(dir.path(), dir, local.dir,cancel);
			} catch (Exception e) {
				throw new EcmException(e);
			}finally{
				if(cancel!=null){
					cancel.setCanceled(true);
				}
			}
		}

	}

	private void downloadFiles(String remoteroot, DirectoryInfo parent,
			File localDir, CancelHandle cancel) throws FileLockException, IOException {
		List<FileInfo> files = parent.listFiles();
		for (FileInfo file : files) {
			if (cancel.isCanceled()) {
				return;
			}
			String remoteRelpath = file.parent().path();
			remoteRelpath = remoteRelpath.substring(remoteroot.length(),
					remoteRelpath.length());
			String remoteFileName = "";
			if (remoteRelpath.endsWith("/")) {
				remoteFileName = String.format("%s%s", remoteRelpath,
						file.name());
			} else {
				remoteFileName = String.format("%s/%s", remoteRelpath,
						file.name());
			}
			String localFileName="";
			String localDirPath=localDir.getPath();
			if(localDirPath.endsWith(File.separator)){
				localFileName=String.format("%s%s", localDirPath,remoteFileName);
			}else{
				localFileName=String.format("%s/%s", localDirPath,remoteFileName);
			}
			downloadFile(file, new File(localFileName),cancel);
		}
		for (DirectoryInfo dir : parent.listDirs()) {
			if (cancel.isCanceled()) {
				return;
			}
			downloadFiles(remoteroot,dir, localDir,cancel);
		}
	}

	private void downloadFile(FileInfo fi, File localFile,CancelHandle cancel)
			throws FileLockException, IOException {
		if(!localFile.getParentFile().exists()){
			localFile.getParentFile().mkdirs();
		}
		System.out.println(String.format("-----%s\t", fi.fullName()));
		System.out.print(String.format("     > %s", localFile.getAbsolutePath()));
		System.out.print('\r');
		
		FileOutputStream out=new FileOutputStream(localFile);
		long pos = 0;
		long len = fi.dataLength();
		int read = 0;
		byte[] b = new byte[256 * 1024];
		IReader reader = fi.reader(0);
		while ((read = reader.read(b, 0, b.length)) > -1) {
			if (cancel.isCanceled()) {
				break;
			}
			out.write(b, 0, read);
			pos += read;
			int per = (int) (((pos * 1.00F) / len) * 100);
			String str = String.format("%s%s", per, '%');
			for (int i = 0; i < str.length(); i++) {
				System.out.print('\b');
			}
			System.out.print(str);
		}
		System.out.print("\r\n");
		reader.close();
		out.close();
		if (cancel.isCanceled()) {
			localFile.delete();
		}
		
	}

	@Override
	public String cmd() {
		return "download";
	}

	@Override
	public String cmdDesc() {
		return "下载文件。将远程当前目录中的文件下载到本地当前目录中。\r\n例：download -f xxx/xxx/fff.file  download -a";
	}

	@Override
	public Options options() {
		Options options = new Options();
		Option file = new Option("f", "file", true, "下载指定文件");
		options.addOption(file);
		Option dir = new Option("a", "all", false, "下载当前远程目录中的所有目录和文件及其下所有");
		options.addOption(dir);
		// Option c = new Option("c", "count", true, "包括统计信息");
		// options.addOption(c);
		// Option p = new Option("p", "password",true, "密码");
		// options.addOption(p);
		return options;
	}
}
