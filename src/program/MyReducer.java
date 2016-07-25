package program;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {

	/**保存离散型属性的所有取值*/
	static ArrayList<String> attVal;
	/**保存attVal中每个属性取值的个数*/
	static ArrayList<Integer> attValNum;
	/**保存属性取值对应的分类的个数*/
	static ArrayList<int[]> classValNum;
	/**保存属性名*/
	static String attributes[];
	/**保存共有多少条数据*/
	static int dataCount;
	/**保存最终分类的取值*/
	static String classVal[];
	/**保存classVal中每个分类的个数*/
	static int classCount[];
	
	/**保存属性是否为连续性取值*/
	static boolean numeric[];
	/**保存连续型属性的所有取值*/
	ArrayList<Double> attConValTmp;
	/**保存连续型属性的对应分类取值*/
	ArrayList<String> attConValClass;
	
	Text myValue = new Text();	//reduce输出的value值
	final double minDataRatio = 0.1;
	
	public void reduce(Text _key, Iterable<Text> values, Context context) 
			throws IOException, InterruptedException {
		// process values
		classCount = new int[classVal.length];
		dataCount = 0;
		boolean isNumeric = false;
		int attIndex = Integer.parseInt(_key.toString().split("&")[1]);
		isNumeric = numeric[attIndex];
		/*for(int i=0;i<attributes.length;i++)
			if(attributes[i].equals(_key.toString())){
				isNumeric = numeric[i];
				break;
			}
		*/
		/*如果是离散型取值的属性*/
		if(!isNumeric){	
			String oneClassVal = "";
			/*每个reduce需要重新申请各自独立的变量，否则如果在全局中声明，
			 * 则所有的reduce将共用同一个存储空间*/	
			attVal = new ArrayList<String>();
			attValNum = new ArrayList<Integer>();
			classValNum = new ArrayList<int[]>();
			/*reduce中对于每一条value数据分别进行处理*/
			for (Text val : values) {
				dataCount++;		//记录数据个数
				//按照map输出格式进行分割字符串
				String split[] = val.toString().split("&");
				oneClassVal = split[1];
				int dex = FindClass(split[1]);		//找到对应类取值
				classCount[dex]++;		//类取值数量加1
				/*如果是第一个输入数据，添加新属性取值，
				 * 其对应的数量设为1，并将其对应的属性分类个数初始化为1*/				
				if(attVal.size()==0){
					attVal.add(split[0]);
					attValNum.add(1);
					InitAttValNum(split[1]);
				}
				/*否则，判断该属性取值是否之前已经出现。如果出现过（通过标记findVal判断），
				 * 则将其对应数量+1，对应的分类个数+1；反之，则表示为出现过，新建相应取值，
				 * 并初始化其数量和对应分类数量信息。*/	
				else{
					boolean findVal = false;
					for(int i=0;i<attVal.size();i++)
						if(attVal.get(i).equals(split[0])){
							findVal = true;
							attValNum.set(i, attValNum.get(i)+1);
							int tmp[] = classValNum.get(i);
							tmp[dex]++;
							break;
						}
					if(!findVal){
						attVal.add(split[0]);
						attValNum.add(1);
						InitAttValNum(split[1]);
					}
				}
			}
			if(dataCount==0)
				return;
			/*通过out字符串输出信息*/
			double gr = GainRatio();
			String valueTmp = "" + gr;
			/*for(int i=0;i<classCount.length;i++)
				if(classCount[i]==dataCount){
					valueTmp += "&" + classVal[i];
					break;
				}*/
			if(gr==0)
				valueTmp += "&" + oneClassVal;
			myValue.set(valueTmp);
			context.write(_key, myValue);
			//System.out.println(_key+" "+valueTmp);
			//System.out.println("Info:"+Info()+"Entr:"+Entr()+"SplitI:"+SplitI());
		}
		
		/*如果是连续型取值的属性*/
		else{
			attConValTmp = new ArrayList<Double>();
			attConValClass = new ArrayList<String>();
			for (Text val : values) {
				dataCount++;
				String split[] = val.toString().split("&");
				int dex = FindClass(split[1]);
				classCount[dex]++;
				attConValTmp.add(Double.parseDouble(split[0]));
				attConValClass.add(split[1]);
			}
			if(dataCount==0)
				return;
			Double[] attConVal = new Double[attConValTmp.size()];
			attConValTmp.toArray(attConVal);
			/*将连续性取值进行快速排序*/
			quickSort(attConVal,0,attConVal.length-1);
			double boundary = -1 , lastBoundary = -1;
			double optBoundary = -1 ,optEntr = 100 ,optGR = -1;
			/*从小到大的遍历连续属性取值，分割成n-1段，判断最佳超出最佳分界值*/
			for(int i=0;i<attConVal.length;i++){
				/*对于每个分界值，都重新初始化属性取值及其对应的个数*/
				attVal = new ArrayList<String>();
				attValNum = new ArrayList<Integer>();
				classValNum = new ArrayList<int[]>();
				/*首次循环，初始化分界值为最小值*/
				boundary = attConVal[i];
				/*当分界值取到连续取值数组中的最大值时，提前结束循环*/
				if(boundary == attConVal[attConVal.length-1])
					break;
				/*相同分界值不重复计算*/
				if(i!=0 && boundary == lastBoundary)
					continue;
				/*更改连续性属性的取值为离散的二值信息，
				 * 分别为大于分界值和小于等于分界值*/
				String nowAttVal;
				int bigCount = 0, smallCount = 0;
				for(int line=0;line<attConValClass.size();line++){
					int dex = FindClass(attConValClass.get(line));	//找到对应类取值
					double cmp = attConValTmp.get(line);
					/*处理当前value中的连续属性取值为对应的离散取值名称,
					 * 并且记录对应的大于或者小于分界值的数量，便于程序之后验证*/
					if(cmp > boundary){
						nowAttVal=">"+boundary;
						bigCount++;
					}
					else{
						nowAttVal="<="+boundary;
						smallCount++;
					}
					/*如果是第一个输入数据，添加新属性取值，其对应的数量设为1，
					 * 并将其对应的属性分类个数初始化为1*/
					if(attVal.size()==0){
						attVal.add(nowAttVal);
						attValNum.add(1);
						InitAttValNum(attConValClass.get(line));
					}
					else{
						boolean findVal = false;
						for(int j=0;j<attVal.size();j++)
							if(attVal.get(j).equals(nowAttVal)){
								findVal = true;
								attValNum.set(j, attValNum.get(j)+1);
								int tmp[] = classValNum.get(j);
								tmp[dex]++;
								break;
							}
						if(!findVal){
							attVal.add(nowAttVal);
							attValNum.add(1);
							InitAttValNum(attConValClass.get(line));
						}
					}
				}
				/*确保分界值分割后的两个数据集的数量都要超过总数据数量的0.1，
				 * 通过常量minDataRatio来设置。*/
				if(smallCount < minDataRatio * dataCount || 
						bigCount < minDataRatio * dataCount )
					continue;
				lastBoundary = boundary;	//保存上次的分界值
				/*如果按照当前分界值分割的信息增益更小，则更新最佳分割信息*/
				double nowEntr = Entr();
				if(nowEntr < optEntr){
					optBoundary	= boundary;
					optEntr = nowEntr;
					optGR = GainRatio();
				}
			}
			String valueTmp = optGR+"&"+optBoundary;
			if(optGR==0)
				valueTmp += "&" + attConValClass.get(0);
			myValue.set(valueTmp);
			context.write(_key, myValue);
		}
	}

	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line=context.getConfiguration().get("CLASS");
		int classNum = line.split(" ").length;
		classVal = new String[classNum];
		for(int i=0;i<classNum;i++)
			classVal[i]=line.split(" ")[i];
		String attTmp=context.getConfiguration().get("ATT");
		String conTmp=context.getConfiguration().get("CON");
		attributes=attTmp.split(":");
		numeric=new boolean[attributes.length];
		for(int i=0;i<conTmp.split(":").length;i++)
			numeric[i]=Boolean.parseBoolean(conTmp.split(":")[i]);
	}
	
	public static void InitAttValNum(String input){
		int out[] = new int[classVal.length];
		for(int i=0;i<classVal.length;i++){
			if(classVal[i].equals(input))
				out[i] = 1;
			else
				out[i] = 0;
		}
		classValNum.add(out);
	}
	
	public static int FindClass(String input){
		int num = -1;
		for(int i=0;i<classVal.length;i++)
			if(classVal[i].equals(input))
				num = i;
		return num;	
	}
	
	public static double Info(){
		double info = 0;
		for(int i:classCount){
			if(i!=0){
				double p = (double)i/dataCount;
				info += (-1)*p*Math.log(p)/Math.log(2);
			}
		}
		return info;
	}
	
	public static double SplitI(){
		double splitI = 0;
		for(int i:attValNum){
			if(i!=0){		
				double p = (double)i/dataCount;
				splitI += (-1)*p*Math.log(p)/Math.log(2);
			}	
		}
		return splitI;
	}
	
	public static double Entr(){
		double entr = 0;
		for(int i=0;i<attValNum.size();i++){
			int valCount = attValNum.get(i);
			double weight = (double)valCount/dataCount;
			double tmp = 0;
			for(int j:classValNum.get(i)){
				if(j!=0){	
					double p = (double)j/valCount;
					tmp += (-1)*p*Math.log(p)/Math.log(2);
				}					
			}
			entr += tmp*weight;
		}
		return entr;
	}
	
	public static double GainRatio(){
		double sp = SplitI();
		if(sp == 0){
			boolean flag = false;
				for(int i : classCount){
					if(i == dataCount){
						flag = true;
						break;
					}
				}
				if(flag)
					return 0;
				else
					return 0.00001;
		}
		return (Info()-Entr())/sp;
	}
	
	public static int getMiddle(Double[] list, int low, int high) {
		double tmp = list[low];    //数组的第一个作为中轴
		while (low < high) {
			while (low < high && list[high] > tmp) {
				high--;
			}
			list[low] = list[high];   //比中轴小的记录移到低端
			while (low < high && list[low] <= tmp) {
				low++;
			}
			list[high] = list[low];   //比中轴大的记录移到高端
		}
		list[low] = tmp;              //中轴记录到尾
		return low;                   //返回中轴的位置
	}
	
	public static void quickSort(Double[] list, int low, int high) {
		if (low < high) {
			int middle = getMiddle(list, low, high);  //将list数组进行一分为二
			quickSort(list, low, middle - 1);        //对低字表进行递归排序
			quickSort(list, middle + 1, high);       //对高字表进行递归排序
		}
	}

}
