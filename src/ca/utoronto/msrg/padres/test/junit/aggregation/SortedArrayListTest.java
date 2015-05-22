package ca.utoronto.msrg.padres.test.junit.aggregation;

import java.util.Iterator;

import junit.framework.TestCase;
import ca.utoronto.msrg.padres.broker.aggregation.utility.SortedArrayList;

public class SortedArrayListTest extends TestCase {

	SortedArrayList<Integer> list;
	
	@Override
	protected void setUp() throws Exception {
		list=new SortedArrayList<Integer>();
		super.setUp();
	}
	
	public void testSimple(){
		
		
		
		list.add(10);
		list.add(1);
		list.add(3);
		list.add(8);
		list.add(0);
		list.add(-10);
		list.add(10);
		
		for(int i=0;i<list.size();i++){
			System.out.println(" "+list.get(i));
		}
			
		System.out.println("\n\n\n\n\n");
		
		for(Integer i:list){
			System.out.println(i);
		}
		
		System.out.println("\n\n\n\n\n");
		
		Iterator<Integer> it = list.iterator();
		
		while(it.hasNext()){
			Integer i= it.next();
			if(i==3){
				it.remove();
			}
		}
		
		System.out.println("\n\n\n\n\n");
		
		for(Integer i:list){
			System.out.println(i);
		}
	}
	
	

}
