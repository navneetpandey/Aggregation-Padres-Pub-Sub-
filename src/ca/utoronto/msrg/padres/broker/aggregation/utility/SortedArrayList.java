package ca.utoronto.msrg.padres.broker.aggregation.utility;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SortedArrayList<E extends Comparable<? super E>> implements Iterable<E> {

	List<E> list = Collections.synchronizedList(new ArrayList<E>());
	
	public synchronized void add(E elt) {
		int position = Collections.binarySearch(list, elt);
		if (position < 0)
			position = -position-1;
		
		list.add(position, elt);
	}
	
	public synchronized boolean contains(E elt){
		if(Collections.binarySearch(list, elt)<0)return false;
		return true;
	}
	
	public E get(int i){
		return list.get(i);
	}
	
	public synchronized void remove(int position){
		list.remove(position);
	}
	
	public int size(){
		return list.size();
	}


	@Override
	public Iterator<E> iterator() {
		return list.iterator();
	}


	public void clear() {
		list.clear();
	}
	
	
}
