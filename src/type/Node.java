package type;

import java.util.ArrayList;

public class Node{
	int father;
	String name;
	String attVal;
	ArrayList<String> value=new ArrayList<String>();
	ArrayList<String> childVal=new ArrayList<String>();
	ArrayList<Integer> child=new ArrayList<Integer>();

	public Node(String n, String[] val){
		father = -1;
		name=new String(n);
		for(int i=0;i<val.length;i++){
			value.add(val[i]);
			child.add(-1);	
		}
	}
}