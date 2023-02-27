# cp-复制

​	`cp ~/tmp/ pg-test/yellow_tripdata_2021-01.csv .`

> The `cp` command is used to copy files or directories from one location to another. In the example you provided, the command is copying a file called `yellow_tripdata_2021-01.csv` from the `/tmp/` directory to a directory called `pg-test`, and then to the current working directory (`.`).

# wget-下载

`wget` is a command-line tool for downloading files from the internet. It can be used to download files from HTTP, HTTPS, and FTP servers. The syntax of the `wget` command is as follows:

```
wget [options] [URL]
```

Here, `[options]` are the various options that can be used with `wget`, and `[URL]` is the URL of the file to be downloaded.

Some commonly used options with `wget` are:

- `-O`: specifies the name of the output file
- `-q`: runs `wget` in quiet mode
- `-c`: continues a previous download
- `-r`: downloads files recursively
- `-np`: avoids downloading files from parent directories
- `-P`: specifies the directory where the downloaded files should be saved

For example, to download a file from a URL using `wget` and save it with a specific name, you can use the following command:

`wget -O filename.extension URL`

This will download the file from the specified URL and save it with the name `filename.extension`.