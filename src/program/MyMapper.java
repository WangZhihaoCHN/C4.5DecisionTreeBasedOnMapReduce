package program;

import type.Rule;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Text myKey = new Text();	//作为map输出的key
	Text myValue = new Text();	//作为map输出的value
	
	String atts[];	//保存所有的属性
	String cons[];	//保存对应属性是否是连续性属性
	
	private static List<Rule> ruleQueue;
	
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		//将按行读入的value值分割
		String split[] = ivalue.toString().split("\t+");
		int attNum = atts.length;
		StringBuilder out;
		for(int rid=0;rid<ruleQueue.size();rid++){
			Rule rule = ruleQueue.get(rid);
			if(isFitRule(rule,split))
				for(int aid=0;aid<attNum;aid++)
					if(!rule.conditions.containsKey(new Integer(aid))){
						/*之前没有使用过，这是一个可能的候选属性。
						 * key键值格式为:规则id&属性id，
						 * 而对应value值格式为:属性取值&所属类*/
						out = new StringBuilder();
						out.append(split[aid]);
						out.append("&");
						out.append(split[attNum]);
						myKey.set(rid+"&"+aid);
						myValue.set(out.toString());
						context.write(myKey, myValue);
						//System.out.println("规则"+rid+"&"+atts[aid]+"\t"+out.toString());
					}
		}
	}

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		ruleQueue = new LinkedList<Rule>();
		// TODO Auto-generated method stub
		String attTmp=context.getConfiguration().get("ATT");
		String conTmp=context.getConfiguration().get("CON");
		atts=attTmp.split(":");
		cons=conTmp.split(":");
		
		URI[] filePath =
				context.getCacheFiles();
		// 因为只有一个文件，所以这个文件应该是Queue
		assert (filePath.length == 1);
		// 载入条件队列信息
		FileSystem fs = FileSystem.get(filePath[0],context.getConfiguration());
		InputStream in = fs.open(new Path(filePath[0]));
		Scanner scanner = new Scanner(in);
		System.err.println("获取的路径是：" + filePath[0].toString());
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			if (line.length() == 0)
				continue;
			Rule rule = Rule.parse(line);
			ruleQueue.add(rule);
		}
		scanner.close();
		if (ruleQueue.size() == 0) {
			// 说明ruleQueue文件是空的，那么此时应该正在处理根节点
			// 我们为根节点生成一个空白的规则，加入队列
			// 确保队列非空
			ruleQueue.add(new Rule());
		}
	}
	
	/** 判断一个样本记录是否符合规则要求 */
	private boolean isFitRule(Rule rule, String[] aValues){
		boolean satisfied = true;
	    for (Integer aid:rule.conditions.keySet()) {
	    	double cmp;
	    	String cmpStr;
	    	//如果是连续性取值，则需要判断其对于分割值的相对大小
	    	if(cons[aid.intValue()].equals("true")){
	    		String get = rule.conditions.get(aid);
	    		if(rule.conditions.get(aid).charAt(0)=='<'){
	    			cmp = Double.parseDouble(get.substring(2, get.length()-1)) ; 
	    		}else{
	    			cmp = Double.parseDouble(get.substring(1, get.length()-1)) ; 
	    		}
	    		if(Double.parseDouble(aValues[aid.intValue()])<=cmp)
	    			cmpStr = "<="+cmp;
	    		else
	    			cmpStr = ">"+cmp;
	    	}else{
	    		cmpStr = aValues[aid.intValue()];
	    	}
		    if (!cmpStr.equals(rule.conditions.get(aid))){
		    	// 如果在某个属性上，该记录元组的值与条件要求的值不符合
		    	satisfied = false;
		    	break;
		    }
	    }
	    return satisfied;
	}
}
