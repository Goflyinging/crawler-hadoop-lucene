package com.lxing.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.mapred.IdentityTableMap;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;

public class BuildTableIndex {
	private static final String USAGE = "Usage: BuildTableIndex "
			+ "-m <numMapTasks> -r <numReduceTasks>\n  -indexConf <iconfFile> "
			+ "-indexDir <indexDir>\n  -table <tableName> -columns <columnName1> " + "[<columnName2> ...]";
	private Scanner sc;

	private static void printUsage(String message) {
		System.err.println(message);
		System.err.println(USAGE);
		System.exit(-1);
	}

	/** default constructor */
	public BuildTableIndex() {
	}

	/**
	 * Block and do not return until the job is complete.
	 * 
	 * @param args
	 * @throws IOException
	 */
	public void run(String[] args) throws IOException {
		// if (args.length < 6) {
		// printUsage("Too few arguments");
		// }

		int numMapTasks = 3;
		int numReduceTasks = 3;
		// String iconfFile = null;
		String indexDir = "lxing";
		String tableName = "CSDN_article";
		StringBuilder columnNames = new StringBuilder("title");
		columnNames.append(" ");
		columnNames.append("content");

		// parse args
		// for (int i = 0; i < args.length - 1; i++) {
		// if ("-m".equals(args[i])) {
		// numMapTasks = Integer.parseInt(args[++i]);
		// } else if ("-r".equals(args[i])) {
		// numReduceTasks = Integer.parseInt(args[++i]);
		// } else if ("-indexConf".equals(args[i])) {
		// iconfFile = args[++i];
		// } else if ("-indexDir".equals(args[i])) {
		// indexDir = args[++i];
		// } else if ("-table".equals(args[i])) {
		// tableName = args[++i];
		// } else if ("-columns".equals(args[i])) {
		// columnNames = new StringBuilder(args[++i]);
		// while (i + 1 < args.length && !args[i + 1].startsWith("-")) {
		// columnNames.append(" ");
		// columnNames.append(args[++i]);
		// }
		// } else {
		// printUsage("Unsupported option " + args[i]);
		// }
		// }

		if (indexDir == null || tableName == null || columnNames == null) {
			printUsage("Index directory, table name and at least one column must " + "be specified");
		}

		Configuration conf = HBaseConfiguration.create();
		// if (iconfFile != null) {
		// // set index configuration content from a file
		// String content = readContent(iconfFile);
		//
		// IndexConfiguration iconf = new IndexConfiguration();
		// // purely to validate, exception will be thrown if not valid
		// iconf.addFromXML(content);
		// conf.set("hbase.index.conf", content);
		// }

		if (columnNames != null) {
			JobConf jobConf = createJob(conf, numMapTasks, numReduceTasks, indexDir, tableName, columnNames.toString());
			RunningJob runningJob = JobClient.runJob(jobConf);
			runningJob.waitForCompletion();
		}
	}

	/**
	 * @param conf
	 * @param numMapTasks
	 * @param numReduceTasks
	 * @param indexDir
	 * @param tableName
	 * @param columnNames
	 * @return JobConf
	 */
	public JobConf createJob(Configuration conf, int numMapTasks, int numReduceTasks, String indexDir, String tableName,
			String columnNames) {
		JobConf jobConf = new JobConf(conf, BuildTableIndex.class);
		jobConf.setJobName("build index for table " + tableName);
		jobConf.setNumMapTasks(numMapTasks);
		// number of indexes to partition into
		jobConf.setNumReduceTasks(numReduceTasks);

		// use identity map (a waste, but just as an example)
		IdentityTableMap.initJob(tableName, columnNames, IdentityTableMap.class, jobConf);

		// use IndexTableReduce to build a Lucene index
		jobConf.setReducerClass(IndexTableReduce.class);
		// 判断文件是否存在 选择删除
		String relativelyPath = System.getProperty("user.dir");
		File file = new File(relativelyPath + File.separator + indexDir);
		if (file.exists()) {
			System.out.print("文件夹已经存在是否删除Y/N:");
			sc = new Scanner(System.in);
			String is = sc.next();
			if (is.equals("Y") || is.equals("y")) {
				File[] f = file.listFiles();
				for (File f1 : f) {
					if (f1.isDirectory()) {
						File[] f2 = f1.listFiles();
						for (File f3 : f2) {
							f3.delete();
						}
						f1.delete();
					} else {
						f1.delete();
					}
				}
				file.delete();
			} else {
				System.exit(0);
			}
			sc.close();
		}

		FileOutputFormat.setOutputPath(jobConf, new Path(indexDir));
		jobConf.setOutputFormat(IndexOutputFormat.class);
		jobConf.setJarByClass(BuildTableIndex.class);
		return jobConf;
	}

	/*
	 * Read xml file of indexing configurations. The xml format is similar to
	 * hbase-default.xml and hadoop-default.xml. For an example configuration,
	 * see the <code>createIndexConfContent</code> method in TestTableIndex
	 * 
	 * @param fileName File to read.
	 * 
	 * @return XML configuration read from file
	 * 
	 * @throws IOException
	 */
	private String readContent(String fileName) throws IOException {
		File file = new File(fileName);
		int length = (int) file.length();
		if (length == 0) {
			printUsage("Index configuration file " + fileName + " does not exist");
		}

		int bytesRead = 0;
		byte[] bytes = new byte[length];
		FileInputStream fis = new FileInputStream(file);

		try {
			// read entire file into content
			while (bytesRead < length) {
				int read = fis.read(bytes, bytesRead, length - bytesRead);
				if (read > 0) {
					bytesRead += read;
				} else {
					break;
				}
			}
		} finally {
			fis.close();
		}

		return new String(bytes, 0, bytesRead, HConstants.UTF8_ENCODING);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		BuildTableIndex build = new BuildTableIndex();
		build.run(args);
	}
}