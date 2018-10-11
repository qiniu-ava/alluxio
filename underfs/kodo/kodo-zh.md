---
layout: global
title: 在KODO上配置Alluxio
nickname: Alluxio使用KODO
group: Under Store
priority: 4
---

* 内容列表
{:toc}

该指南介绍如何配置 Alluxio 以使用[Qiniu KODO](https://www.qiniu.com/products/kodo)作为底层文件系统。七牛云对象存储 Kodo 是七牛云提供的高可靠、强安全、低成本、可扩展的存储服务。

## 初始步骤

要在许多机器上运行 Alluxio 集群，需要在这些机器上部署二进制包。你可以自己[编译Alluxio](Building-Alluxio-From-Source.html)，或者[下载二进制包](Running-Alluxio-Locally.html)

另外，为了在 KODO 上使用 Alluxio，需要创建一个bucket（或者使用一个已有的bucket）。还要注意在该 bucket 里使用的目录，可以在该bucket中新建一个目录，或者使用一个存在的目录。在该指南中，Kodo bucket的名称为KODO_BUCKET，在该 bucket 里的目录名称为 KODO_DIRECTORY。要使用七牛对象存储服务，需要提供一个可供识别指定 bucket 的域名，本向导中为 KODO_DOWNLOAD_HOST。
除此之外，还需提供一个 KODO 端点，该端点指定了你的 bucket 在哪个范围，本向导中的端点名为 KODO_ENDPOINT。

## 安装Kodo

Alluxio通过[统一命名空间](Unified-and-Transparent-Namespace.html)统一访问不同存储系统。 Kodo的安装位置可以在Alluxio命名空间的根目录或嵌套目录下。

### 根目录安装

若要在Alluxio中使用七牛Kodo作为底层文件系统，一定要修改`conf/alluxio-site.properties`配置文件。首先要指定一个已有的Kodo bucket和其中的目录作为底层文件系统，可以在`conf/alluxio-site.properties`中添加如下语句指定它：

```
alluxio.underfs.address=kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```

接着，需要一些配置访问Kodo，在`conf/alluxio-site.properties`中添加：

```
fs.kodo.AccessKey=<KODO_ACCESS_KEY>
fs.kodo.SecretKey=<KODO_SECRET_KEY>
fs.kodo.DownloadHost=<KODO_DOWNLOAD_HOST>
fs.kodo.EndPoint=<KODO_ENDPOINT>
```

此处, `fs.kodo.AccessKey `和`fs.kodo.SecretKey`分别为`Access Key`字符串和`Secret Key`字符串，均受七牛云[密钥管理界面](https://portal.qiniu.com/user/key)管理；

`fs.kodo.DownloadHost` 可以在[七牛云对象存储管理平台](https://portal.qiniu.com/bucket) 中的空间概览中获取[访问测试域名](https://mars-assets.qnssl.com/alluxio_host.png)

`fs.kodo.endpoint` 是七牛云存储源站的端点配置 可以根据 bucket 所在区域使用对于的端点

具体关于 Kodo 对应的 EndPoint 可以参考

| 存储区域 | 地域简称 | EndPoint |
| ------- | -------- | --------- |
|华东| z0|  iovip.qbox.me | 
|华北| z1| iovip-z1.qbox.me| 
|华南| z2| iovip-z2.qbox.me | 
|北美| na0| iovip-na0.qbox.me | 
|东南亚| as0| iovip-as0.qbox.me | 

更改完成后，Alluxio应该能够将Kodo作为底层文件系统运行，你可以尝试

### 嵌套目录安装

Kodo可以安装在Alluxio命名空间中的嵌套目录中，以统一访问多个存储系统。 
[Mount 命令](Command-Line-Interface.html#mount)可以实现这一目的。例如，下面的命令将Kodo容器内部的目录挂载到Alluxio的`/kodo`目录

```bash 
$ ./bin/alluxio fs mount --option fs.kodo.AccessKey=<KODO_ACCESS_KEY> \
  --option fs.kodo.SecretKey=<KODO_SECRET_KEY> \
  --option fs.kodo.DownloadHost=<KODO_DOWNLOAD_HOST> \
  --option fs.kodo.EndPoint=<KODO_ENDPOINT> \
  kodo://<KODO_BUCKET>/<KODO_DIRECTORY>//
```

## 使用KODO在本地运行Alluxio

配置完成后，你可以在本地启动 Alluxio，观察一切是否正常运行：

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

该命令应当会启动一个 Alluxio master 和一个 Alluxio worker，可以在浏览器中访问[http://localhost:19999](http://localhost:19999)查看master UI。

接着，你可以运行一个简单的示例程序：

```bash
$ bin/alluxio runTests
```

运行成功后，访问你的Kodo目录`KODO://<KODO_BUCKET>/<KODO_DIRECTORY>`，确认其中包含了由Alluxio创建的文件和目录。在该测试中，创建的文件名称应像`KODO_BUCKET/KODO_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`这样。

运行以下命令停止 Alluxio：

```bash
$ bin/alluxio-stop.sh local
```

