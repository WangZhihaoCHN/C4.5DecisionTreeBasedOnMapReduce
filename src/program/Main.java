package program;

import type.Rule;
import type.Node;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author hadoop
 *
 */
public class Main {
	/*记录属性输入文件中，有关本次测试集合的属性个数，名称和是否为连续型*/
	static ArrayList<String> att = new ArrayList<String>();
	static ArrayList<String> con = new ArrayList<String>();
	static ArrayList<String[]> attVal = new ArrayList<String[]>();
	static ArrayList<String> classVal = new ArrayList<String>();
	
	static ArrayList<Node> tree = new ArrayList<Node>();
	static Queue<Rule> model = new LinkedList<Rule>();
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	    if (otherArgs.length != 4) {
	    	System.err.println("Usage: wordcount <InputPath> <OutputPath> <AttPath> <TmpPath>");
	    	System.exit(2);
	    }
	    //读取attributes文件夹下的属性信息
	    getAttInfo(new Path(otherArgs[2]));
	    //当前层的划分规则队列
	    Queue<Rule> currentQueue = new LinkedList<Rule>();
	    //下一层的划分规则队列
	    //Queue<Rule> newQueue = new LinkedList<Rule>();
	    //是否包含Root节点，用于甄别是否是初始执行
	    boolean hasRootNode = false;
	    //当前迭代的轮数，即决策树的深度
	    int iterateCount = 0;
	    //不断的向深层扩展决策树，直到无法继续构建为止
	    while(true){
	    	//增加一轮迭代
	        iterateCount++;
	        // 准备当前轮迭代的环境变量数据
	        String queueFilePath = otherArgs[3] + "/queue" + iterateCount;
	        String newstatisticFilePath =otherArgs[1] + "/static" + iterateCount;
	        //将NewQueue中保存的当前轮迭代所在的层的节点信息写入文件
	        writeNewRuleToQueueFile(currentQueue, queueFilePath);
	        //将当前层的队列指针指向上一轮迭代的输出
	        //currentQueue = newQueue;
	        //判断是否有根节点，对于根节点需要单独处理
	        if(!hasRootNode){
	        	//向Queue中插入一个空白的节点，作为根节点
	            Rule rule = new Rule();
	            currentQueue.add(rule);
	            hasRootNode = true;
	        }
	        /*判断一下当前层数上是否有新的节点可以生长。如果已经没有新的节点供生长了，
	         * 决策树模型已经构造完成,退出构造while循环*/
	        if(currentQueue.isEmpty())
	        	break;
	        /*继续运行，说明当前层还有节点可供生长。
	         * 运行MapReduce作业，对当前层节点分裂信息进行统计*/
	        runMapReduce(otherArgs[0], queueFilePath, 
	        		newstatisticFilePath, iterateCount);
	        //从输出结果中读取统计好的信息
	        int optAttIndex[] = new int[currentQueue.size()];
			for(int i=0;i<optAttIndex.length;i++)
				optAttIndex[i]=-2;
		    getOptAttIndex(optAttIndex,newstatisticFilePath);
		    for(int i=0;i<optAttIndex.length;i++){
		    	if(optAttIndex[i]==-1)
			    	System.out.println("第"+iterateCount+"轮迭代,规则"+i
			    			+"应该为:叶子节点");	
		    	else if(optAttIndex[i]!=-2)
			    	System.out.println("第"+iterateCount+"轮迭代,规则"+i
			    			+"应该选择的属性为："+att.get(optAttIndex[i]));	
		    }
		    int currentLength = currentQueue.size();
	        for(int i=0;i<currentLength;i++){
	        	Rule rule = currentQueue.poll();
	        	if(optAttIndex[i]==-2)
	        		continue;
	        	if(optAttIndex[i]==-1){
	        		Rule newRule = new Rule();
	        		newRule.conditions = new HashMap<Integer, String>(rule.conditions);
	        		newRule.label = seafVal[i];
	                model.add(newRule);
	                continue;// 继续处理下一个当前层的节点
	        	}
	        	//System.out.println("optAttIndex[i]："+optAttIndex[i]);
	        	for(String attValTmp : attVal.get(optAttIndex[i])){
	        		Rule newRule = new Rule();
	        		newRule.conditions = new HashMap<Integer, String>(rule.conditions);
	        		newRule.conditions.put(optAttIndex[i], attValTmp);
	        		newRule.label = "";
	        		currentQueue.add(newRule);
	        	}
	        }
	        System.out.println("新一次迭代规则队列长度为："+currentQueue.size());
	        System.out.println("下一次迭代使用的规则：");
	        for(Rule rule:currentQueue){
		    	System.out.println(rule.toString());
		    }
	    }
	    System.out.println();
	    System.out.println("****************************");
	    System.out.println("********* 程序结束！ *********");
	    System.out.println("****************************");
	    System.out.println("最终的规则为：");
	    for(Rule rule:model){
	    	System.out.println("  "+rule.toString());
	    }
	}
	
	/*	通过路径attFile文件的属性信息
	 * 	（其中包括相关属性的名称、取值、是否为连续性取值以及最终分类的名称），
	 * 	处理后，将相关信息添加到对应的变量，便于之后传给map以及reduce使用。*/
	public static void getAttInfo(Path attFile){
		Configuration conf = new Configuration();
		try{
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream istream = fs.open(attFile);
			Scanner scanner = new Scanner(istream);
			while (true) {
				String line = scanner.nextLine();
				if(scanner.hasNextLine()){
					att.add(line.split(":")[0]);
					con.add(line.split(":")[1]);	
					if(line.split(":")[1].equals("string")){
						String splitAttVal[] = line.split(":")[2].split(",");
						attVal.add(splitAttVal);
					}else
						attVal.add(null);
				}
				else{
					String splitClassVal[] = line.split(":")[1].split(",");
					for(String claVal:splitClassVal)
						classVal.add(claVal);
					break;	
				}
			}
			scanner.close();
			istream.close();
		}catch (IOException e){
			e.printStackTrace();
		}
	}
	
	public static void runMapReduce(String dataSetPath,
			String nodeRuleQueueFilePath, String statisticFilePath, int iterateCount) 
			throws Exception{
		Configuration conf = new Configuration();
		//将获得的属性相关信息，通过context告知map，方便后期处理
	    StringBuilder atts = new StringBuilder();
	    StringBuilder cons = new StringBuilder();
	    for(int i=0;i<att.size();i++){
	    	atts.append(att.get(i)+":");
	    	if(con.get(i).equals("numeric"))
	    		cons.append("true:");
	    	else
	    		cons.append("false:");
	    }
	    conf.set("ATT", atts.toString());
	    conf.set("CON", cons.toString());
	    String classes = "";
	    for(String s:classVal)
	    	classes += (s+" ");
	    conf.set("CLASS", classes);
		Job job = Job.getInstance(conf, "C45");
		job.addCacheFile(new Path(nodeRuleQueueFilePath).toUri());
		job.setJarByClass(Main.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		Path outputPath = new Path(statisticFilePath);
	    FileInputFormat.addInputPath(job, new Path(dataSetPath));
	    FileOutputFormat.setOutputPath(job, outputPath);
	    /*如果在HDFS上存在这个目录，则删除该目录。这个存在的目录会导致作业失败。*/
	    outputPath.getFileSystem(conf).delete(outputPath, true);
	    
	    job.waitForCompletion(true);  
	}
	
	/*通过attName输入属性的姓名，寻找其在全局变量att中的下标；
	 * 找到则讲下标作为返回值，否则返回-1.*/
	/*public static int getAttIndex(String attName){
		for(int i=0;i<att.size();i++){
			if(att.get(i).equals(attName))
				return i;
		}
		return -1;
	}*/
	static String seafVal[];
	public static void getOptAttIndex(int optIndex[], String outputFile){
		double maxGR[] = new double[optIndex.length];
		seafVal = new String[optIndex.length];
		try{
			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
		    FileStatus[] files = fs.listStatus(new Path(outputFile));
		    Path[] pathes = FileUtil.stat2Paths(files);
		    for(Path path : pathes){
		    	if (path.getName().startsWith("_"))
		            continue;
		    	FSDataInputStream istream = fs.open(path);
				Scanner scanner = new Scanner(istream);
				int ruleIndex = -1;
				int nowIndex = -1;
				double nowGR = -1;
				while (scanner.hasNextLine()) {
					String line = scanner.nextLine();
					String split[] = line.split("\\t+");
					ruleIndex = Integer.parseInt(split[0].split("&")[0]);
					nowIndex = Integer.parseInt(split[0].split("&")[1]);
					/*如果是连续性属性*/
					if(con.get(nowIndex).equals("numeric")){
						nowGR = Double.parseDouble(split[1].split("&")[0]);
						double optBoundary = Double.parseDouble(split[1].split("&")[1]);
						String s[] = new String[2];
						s[0] = "<=" + optBoundary;
						s[1] = ">" + optBoundary;
						attVal.set(nowIndex, s);
					}else{
						nowGR = Double.parseDouble(split[1].split("&")[0]);
					}
					if(nowGR==0){
						if(con.get(nowIndex).equals("numeric"))
							seafVal[ruleIndex] = split[1].split("&")[2];
						else
							seafVal[ruleIndex] = split[1].split("&")[1];
						optIndex[ruleIndex] = -1;
						continue;
					}
					if(nowGR > maxGR[ruleIndex]){
						maxGR[ruleIndex] = nowGR;
						optIndex[ruleIndex] = nowIndex;
					}
				}
				scanner.close();
				istream.close();
		    }
		}catch (IOException e){
			e.printStackTrace();
		}
	}
	
	// 将队列中的子节点规则信息输出到文件中
	static void writeNewRuleToQueueFile(Queue<Rule> queue,
			String filePath) {
		Configuration conf = new Configuration();
	    try {
	    	FileSystem fs = FileSystem.get(conf);
	    	Path path = new Path(filePath);
	    	FSDataOutputStream ostream = fs.create(path);
	    	PrintWriter printWriter = new PrintWriter(ostream);
	    	//输出队列中的信息
	    	for(Rule rule : queue) {
	    		printWriter.println(rule.toString());
	    	}
	    	printWriter.close();
	    	ostream.close();
	    } catch (IOException e) {
	    	e.printStackTrace();
	    }
	}
	
}
