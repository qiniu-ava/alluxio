---
layout: global
title: Configuring Alluxio with Kodo
nickname: Alluxio with Kodo
group: Under Store
priority: 4
---

* Table of Contents
{:toc}

This guide describes how to configure Alluxio with
[Qiniu KODO](https://www.qiniu.com/products/kodo) as the under storage system. Object Storage
Service (Kodo) is a massive, secure and highly reliable cloud storage service provided by Qiniu.

## Initial Setup

To run an Alluxio cluster on a set of machines, you must deploy Alluxio binaries to each of these
machines.You can either
[compile the binaries from Alluxio source code](Building-Alluxio-From-Source.html),
or [download the precompiled binaries directly](Running-Alluxio-Locally.html).

Also, in preparation for using Kodo with alluxio, create a bucket or use an existing bucket. You
should also note the directory you want to use in that bucket, either by creating a new
directory in the bucket, or using an existing one. For the purposes of this guide, the Kodo bucket
name is called `KODO_BUCKET`, and the directory in that bucket is called `KODO_DIRECTORY`. To use the seven cattle object storage service, we need to provide a domain name to identify the specified bucket,for name is called `KODO_DOWNLOAD_HOST` .Also, for
using the Qiniu Service, you should provide an Kodo endpoint to specify which range your bucket is
on. The endpoint here is called `Kodo_ENDPOINT`, 

## Mounting Kodo

Alluxio unifies access to different storage systems through the
[unified namespace](Unified-and-Transparent-Namespace.html) feature. An Kodo location can be
either mounted at the root of the Alluxio namespace or at a nested directory.

### Root Mount

You need to configure Alluxio to use Kodo as its under storage system. The first modification is to
specify an existing Kodo bucket and directory as the under storage system by modifying
`conf/alluxio-site.properties` to include:

```
alluxio.underfs.address=kodo://<KODO_BUCKET>/<KODO_DIRECTORY>/
```

Next you need to specify for Kodo access. In `conf/alluxio-site.properties`,
add:

```

fs.kodo.AccessKey=<KODO_ACCESS_KEY>

fs.kodo.SecretKey=<KODO_SECRET_KET>

fs.kodo.DownloadHost=<KODO_DOWNLOAD_HOST>

fs.kodo.EndPoint=<KODO_ENDPOINT>

```

Here `fs.kodo.AccessKey `  and `fs.kodo.SecretKey` is the Access
Key Secret string, which are managed in [AccessKeys](https://portal.qiniu.com/user/key) in Qiniu Portal Manager.
`fs.kodo.DownloadHost`  can be found in [Qiniu Kodo](https://portal.qiniu.com/bucket) 
according to this [order](https://mars-assets.qnssl.com/alluxio_host.png)

`fs.kodo.EndPoint` is the endpoint of this bucket, which can be found in the Bucket in this table

| Region | Abbreviation| EndPoint |
| ------- | -------- | --------- |
|East China| z0|  iovip.qbox.me | 
|North China| z1| iovip-z1.qbox.me| 
|South China| z2| iovip-z2.qbox.me | 
|North America| na0| iovip-na0.qbox.me | 
|Southeast Asia| as0| iovip-as0.qbox.me |

After these changes, Alluxio should be configured to work with Kodo as its under storage system,
and you can try to run alluxio locally with Kodo.

### Nested Mount
An Kodo location can be mounted at a nested directory in the Alluxio namespace to have unified
access to multiple under storage systems. Alluxio's
[Mount Command](Command-Line-Interface.html#mount) can be used for this purpose.
For example, the following command mounts a directory inside an Kodo bucket into Alluxio directory

```bash 
$ ./bin/alluxio fs mount --option fs.kodo.AccessKey=<KODO_ACCESS_KEY> \
  --option fs.kodo.SecretKey=<KODO_SECRET_KET> \
  --option fs.kodo.DownloadHost=<KODO_DOWNLOAD_HOST> \
  --option fs.kodo.EndPoint=<KODO_ENDPOINT> \
  kodo://<KODO_BUCKET>/<KODO_DIRECTORY>//
```

## Running Alluxio Locally with Kodo

After everything is configured, you can start up Alluxio locally to see that everything works.

```bash
$ bin/alluxio format
$ bin/alluxio-start.sh local
```

This should start an Alluxio master and an Alluxio worker. You can see the master UI at
[http://localhost:19999](http://localhost:19999).

Next, you can run a simple example program:

```bash
$ bin/alluxio runTests
```

After this succeeds, you can visit your Kodo directory `KODO://<KODO_BUCKET>/<KODO_DIRECTORY>` to verify the files
and directories created by Alluxio exist. For this test, you should see files named like
`KODO_BUCKET/KODO_DIRECTORY/default_tests_files/BasicFile_CACHE_PROMOTE_MUST_CACHE`.

To stop Alluxio, you can run:

```bash
$ bin/alluxio-stop.sh local
```
