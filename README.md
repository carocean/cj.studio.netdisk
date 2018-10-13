#Netdisk
- 网盘工具
- 基于mongodb实现
- 支持结构化存储
- 支持文件存储
- 支持用户空间配额

###章节
- **netdisk 多维云盘命令行工具**

- **netdisk 多维云盘开发工具包**



##netdisk 多维云盘命令行工具
**摘要：**mdisk是多维网盘的客户端命令行工具，只要你安装好了mongodb，将命令行工具连接上它就可以开始使用网盘了。

下载mdisk，如欲使用最新版，到开发平台下载 
**正文：**
 

![目录](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/00.png)

![目录](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/1.png)


![查看帮助](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/0.png)

![连接本地的mognodb](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/2.png)

![列出网盘](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/3.png)

![网盘命令](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/4.png)


![打开网盘](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/5.png)

![打开存储空间](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/6.png)

![列出元数据](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/7.png)

![列出元组数据](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/7.png)

![元组命令集](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/8.png)

![进入文件系统](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/9.png)

![进入文件系统](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/9.png)

![进入目录列出文件](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/10.png)

![进入本地文件系统](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/11.png)

![列出本地文件](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/12.png)

![列出本地命令集](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/13.png)

![上传文件到网盘](https://github.com/carocean/cj.studio.netdisk/blob/master/document/img/14.png)

##netdisk 多维云盘开发工具包

**摘要：**netdisk可以看成mongodb的一个增强工具包，它实现了用户空间配额、orm映射、类sql的查询、文件的存储、多维的方案等功能。 本包提供开发者使用多维云盘的能力，如果想作为成品或进行云盘的管理和测试请使用netdisk命令行工具 
  正文： 本文简要说明netdisk的使用 简要来说，一个mongodb实例可以被分成多个网盘netdisk,每个netdisk拥有一个或多个cube（即存储方案），其中只有一个名为home的cube，home在netdisk是主cube. 而每个cube里，包含两种数据，一种是结构化数据，称之为tuple，不定数目；一种是非结构化数据，在每个cube.fileSystem()中得到，即每个cube有一个文件系统。 记住，不论是tuple还是文件都是多维的，因此都有多维的api 
  1. 创建、销毁云盘接口

	INetDisk disk=NetDisk.create(client, name, userName, password, info)//创建一个网盘 
	INetDisk disk=NetDisk.open(client, name, userName, password)//认证打开网盘 
	INetDisk disk=NetDisk.trustOpen(client, name)//授信打开网盘

  2. cube的使用

	ICube cube=disk.cube("cubename"); 
	ICube cube=disk.home(); //以下是查询一个叫“department"的tuple 
	IQuery q=cube.createQuery("select {'tuple':'*'} from tuple department your.crop.Department where {'tuple.name':'?(name)'}");
	  q.setParameter('name','研发部');
	  List list=q.getResultList(); 
	  for(IDocument doc:list){ 
	  	System.out.println(doc.docid()); 
	  	System.out.println(doc.tuple()); 
	  } 
	  //以下是保存一个部门：
	  Department dpt=new Department(); 
	  cube.saveDoc('department',new TupleDocument(dtp));
	  //以下是访问文件系统 
	  FileSystem fs=cube.fileSystem(); 
	  DirectoryInfo dir=fs.dir("/test");
	  //打开一个目录 
	  dir.listFiles();//列出文件
	  FileInfo file=fs.openFile("/test/b.txt", OpenMode.openOrNew, OpenShared.off);//打开一个文件 
	  IReader reader=file.reader(0);//从位置0开始读取文件数据 
	  reader.read(buf); 
	  reader.seek(100);//定位到100开始读 
	  reader.read(buf); 
	  reader.close(); 
	  IWriter writer=file.writer(0);//从位置0开始写入数据 
	  writer.write(buf); 
	  writer.close();
