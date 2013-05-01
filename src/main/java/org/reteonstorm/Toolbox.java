package org.reteonstorm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class Toolbox {

	public static LinkedHashSet<String[]> extractFilterGroup(String[][] FILTER_ARRAYS, Set<Integer> group) {
		LinkedHashSet<String[]> filters = new LinkedHashSet<String[]>();
		for (int i : group){
			String[] foo = FILTER_ARRAYS[i];
			filters.add(foo);
		}
		return filters;
	}

	public static TreeSet<String> commonVars(Set<String[]> group) {
		TreeSet<String> intersection = new TreeSet<String>();
		Iterator<String[]> iter = group.iterator();
		intersection.addAll(Toolbox.extractVars(iter.next())); //assumes there exists at least one group filter in "group"
		while (iter.hasNext())
			intersection.retainAll(Toolbox.extractVars(iter.next()));
		return intersection;
	}

	public static TreeSet<String> unionAll(Collection<Set<String>> sets) {
		TreeSet<String> union = new TreeSet<String>();
		for (Set<String> set : sets)
			union.addAll(set);
		return union;
	}

	public static Set<String> extractVars(String[] array) {
		TreeSet<String> vars = new TreeSet<String>();
		for (String part : array)
			if (part.startsWith(TopologyMain.VAR_INDICATOR))
				vars.add(part);
		return vars;
	}

	public static TreeSet<String> unionVarsUpTo(String[][] filters, int lastIndex) { //TODO use libraries
		TreeSet<String> union = new TreeSet<String>();
		for (int i=0; i<=lastIndex; i++)
			for (String vars : extractVars(filters[i]))
				union.add(vars);
		//		String[] result = new String[union.size()];
		//		int i=0;
		//		for (String s : union)
		//			result[i++] = s;
	
		return union;
	}

	public static int deepEqualsIndexOf(List<String[]> haystack, String[] needle) {
		for (int i=0; i<haystack.size(); i++){
			boolean equal = true;
			for (int j=0; j<TopologyMain.FILTER_LENGTH; j++){
				if (!haystack.get(i)[j].equals(needle[j])){
					equal = false;
				}
			}
			if (equal) return i;
		}
		return -1;
	}

	public static TreeSet<String> commonVars(String[] left, String[] right){
		TreeSet<String> intersection = new TreeSet<String>();
		intersection.addAll(extractVars(left));
		intersection.retainAll(extractVars(right));
		return intersection;
	}

	public static Set<String> setIntersection(Set<String> left, Set<String> right) {
		Set<String> clone = new TreeSet<String>(left);
		clone.retainAll(right);
		return clone;
	}

	public static Map<String,String> mapUnion(Map<String,String> left, Map<String,String> right){
		Map<String,String> toEmit = new HashMap<String, String>();
		toEmit.putAll(left);
		toEmit.putAll(right);
		return toEmit;
	}

	public static String extractFieldsGroupingString(Map<String,String> combinedBindings, List<String> fieldsGroupingVars){
		Set<String> vars = new TreeSet<String>(combinedBindings.keySet());
		vars.retainAll(fieldsGroupingVars);
		Collection<String> values = new ArrayList<String>(0);
		for (String var : vars)
			values.add(combinedBindings.get(var));
		return Toolbox.myToString(values);
	}

	public static String myToString(Collection<String> values) { //TODO find a nicer way
		StringBuilder strBuild = new StringBuilder();
		for (String value : values)
			strBuild.append(value);
		return strBuild.toString();
	}

	public static Set<String> setUnion(Set<String> left, Set<String> right) {
		Set<String> union = new TreeSet<String>();
		union.addAll(left);
		union.addAll(right);
		return union;
	}

	public static List<String> toList(String[] strings) {
		List<String> list = new ArrayList<String>(strings.length);
		for (String string : strings)
			list.add(string);
		return list;
	}

}
