package fastppv.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import fastppv.util.KeyValuePair;
import fastppv.util.MapCapacity;

public class PPV {

	protected Map<Integer, Double> map;
	
	public PPV(int numNodes) {
		map = new HashMap<Integer, Double>(MapCapacity.compute(numNodes));
	}
	
	public PPV() {
		map = new HashMap<Integer, Double>();
	}
	

	public Set<Entry<Integer, Double>> getEntrySet() {
		return map.entrySet();
	}
	
	
	public double get(int id) {
		Double d = map.get(id);
		if (d == null)
			return 0;
		else
			return d;
	}
	
	public void set(int id, double value) {
		map.put(id, value);
	}
	
	public void addFrom(PPV ppv, double scale) {
		for (int i : ppv.map.keySet()){
			//System.out.println("hub community= "+community + "node "+ i+ "community: "+ communityMap.get(i));
			//if(community == communityMap.get(i))
				this.set(i, this.get(i) + ppv.get(i) * scale);
		}
			
	}
	
	public PPV duplicateScale(double scale) {
		PPV ppv = new PPV(this.map.size());
		for (int i : this.map.keySet())
			ppv.set(i, this.get(i) * scale);
		return ppv;
	}
	
	public PPV duplicate() {
		PPV ppv = new PPV();
		ppv.map.putAll(this.map);
		return ppv;
	}
	
	public void addFrom(PPV ppv) {
		for (int i : ppv.map.keySet())
			this.set(i, this.get(i) + ppv.get(i));
	}
	

	public List<KeyValuePair> getTopResult(int k) {
		List<Integer> list = new ArrayList<Integer>(map.size());
		for (int i : map.keySet())
			list.add(i);
		
		Collections.sort(list, new Comparator<Integer>() {
			@Override
			public int compare(Integer arg0, Integer arg1) {
				return - Double.compare(get(arg0), get(arg1));
			}} );
		
		if (k > list.size()) 
			k = list.size();
		
		List<KeyValuePair> result = new ArrayList<KeyValuePair>(k);

		for (int i = 0; i < k; i++) {
			result.add(new KeyValuePair(list.get(i), get(list.get(i))));
		}
		
		return result;
	}
	
}


