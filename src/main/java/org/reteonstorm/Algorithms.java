package org.reteonstorm;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.reteonstorm.bolts.CounterTerminal;
import org.reteonstorm.bolts.MultiJoinDoubleMemories;
import org.reteonstorm.bolts.SimpleJoinDoubleMemory;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class Algorithms {

	public static Set<Set<Integer>> groupFilters(String[][] filterArrays) {
		//if there are two common variables between any two members in a group, then those should be common with all members of the group
		//each group member must be checked before a filter is added to the group because the new filter might have something in common with a group member that has not been common between group members so far 

		Set<Set<Integer>> groupOfFilters = new LinkedHashSet<Set<Integer>>();

		TreeSet<Integer> firstGroup = new TreeSet<Integer>();
		firstGroup.add(0);
		groupOfFilters.add(firstGroup);

		filters:for (int i=1; i<filterArrays.length; i++){

			groups:for (Set<Integer> group : groupOfFilters){

				Iterator<Integer> groupIter = group.iterator();

				int firstGroupMember = groupIter.next();
				TreeSet<String> commonVars = Toolbox.intersection(filterArrays[firstGroupMember], filterArrays[i]);
				if (commonVars.isEmpty())
					continue groups;

				while(groupIter.hasNext()){
					int currentGroupMember = groupIter.next();
					TreeSet<String> currentCommonVars = Toolbox.intersection(filterArrays[currentGroupMember], filterArrays[i]);
					if (!currentCommonVars.equals(commonVars))
						continue groups;
				}
				//reached this point => common vars between this filter and the first group member are the same as the common vars with every other group member 
				group.add(i);
				continue filters;
			}
			//reached this point => filter couldn't be added to any of the existing groups
			TreeSet<Integer> newGroup = new TreeSet<Integer>();
			newGroup.add(i);
			groupOfFilters.add(firstGroup);
		}

		return groupOfFilters;
	}

	//	//Determine Joins
	//	Set<Set<Integer>> groupsOfFilters = new LinkedHashSet<Set<Integer>>();
	//	Map<String, List<String>> fieldsGroupingVarsPerStream = new TreeMap<String, List<String>>();
	//	if (shareJoins){
	//		String[][] filtersToBeGrouped = FILTER_ARRAYS.clone();
	//
	//		while(totalSize(groupsOfFilters) < FILTER_ARRAYS.length){
	//			// TODO: check inveriant: number of non-null entries in filtersToBeGrouped + totalSize(groupsOfFilters) should always be equal to FILTER_ARRAYS.length
	//			Set<Integer> selectedFilters = new TreeSet<Integer>();
	//			Set<String> selectedFiltersCommonVars = new TreeSet<String>();
	//			outter: for (int i=0; i<filtersToBeGrouped.length; i++){
	//				if (filtersToBeGrouped[i] == null)
	//					continue outter;
	//				Set<String> commonVars = new TreeSet<String>();
	//				Set<Integer> filters = new TreeSet<Integer>();
	//				filters.add(i);
	//				inner: for (int j=i+1; j<filtersToBeGrouped.length; j++){
	//					if (filtersToBeGrouped[j] == null)
	//						continue inner;
	//					Set<String> innerCommonVars = MultiJoinDoubleMemories.intersection(extractVars(filtersToBeGrouped[i]),extractVars(filtersToBeGrouped[j]));
	//					if (innerCommonVars.size() > 0 && innerCommonVars.equals(commonVars)){
	//						filters.add(j);
	//					}else if (commonVars.isEmpty() || innerCommonVars.size() > commonVars.size()){
	//						commonVars = innerCommonVars;
	//						filters = new TreeSet<Integer>();
	//						filters.add(i);
	//						filters.add(j);
	//					}//else continue
	//				}
	//				/*
	//				 * At this point, out of the filters with the BIGGEST NUMBER OF COMMON VARIABLES with the current filter, the BIGGEST SET OF FILTERS that have common variables with the current filter has been found
	//				 * examples of extracted group out of set of filters:
	//				 * 	?a_foo_?b ?b_bar_?a ?a_foo_1 => ?a_foo_?b ?a_bar_?b
	//				 * 	?a_foo_?c ?a_bar_?b ?a_foo_0 ?b_baar_?c => ?a_foo_?c ?a_bar_?b ?a_foo_0
	//				 * 
	//				 * The extracted group is subtracted from the original set, and the process is repeated until no filters are left 
	//				 */
	//				if (commonVars.size() > selectedFiltersCommonVars.size() || 
	//						commonVars.size() == selectedFiltersCommonVars.size() && filters.size() >= selectedFilters.size()){
	//					selectedFilters = filters;
	//					selectedFiltersCommonVars = commonVars;
	//				}
	//			}
	//			groupsOfFilters.add(selectedFilters);
	//			for (int filter : selectedFilters)
	//				filtersToBeGrouped[filter]=null;
	//		}

	//	}
	//	//out of scope: could add dangling Cartesian filter in one of the groups. How about Cartesian between two large groups (or other order issues)

	public static void shareSimilarFilters(String[][] filterArrays, List<String[]> filtersToAdd, GlobalStreamId[] streams) {
		/*  Filters that produce the same output are combined into a single filter,
		 *  even if they have different variable names */

		List<String[]> correspondingVars = new ArrayList<String[]>(filterArrays.length);

		for (int i=0; i<filterArrays.length; i++){
			String[] filter = filterArrays[i];
			String[] varIndependentFilter = filter.clone();

			//remove variable names from filter clone
			String[] vars = new String[filter.length];
			for (int j=0; j<TopologyMain.FILTER_LENGTH; j++)
				if (filter[j].startsWith(TopologyMain.VAR_INDICATOR)){
					vars[j]=filter[j];
					varIndependentFilter[j]=TopologyMain.DEFAULT_VAR;
				}

			//collect any variables found in this filter
			int index;
			if ((index=Toolbox.deepEqualsIndexOf(filtersToAdd, varIndependentFilter)) >= 0)
				for (int j=0; j<TopologyMain.FILTER_LENGTH; j++)
					correspondingVars.get(index)[j]+=vars[j];
			else{
				filtersToAdd.add(varIndependentFilter);
				correspondingVars.add(vars);
				index = correspondingVars.size()-1;
			}
			streams[i]=new GlobalStreamId(TopologyMain.FILTER_PREFIX+index, TopologyMain.DEFAULT_STREAM_NAME);
		}

		//join filtersToAdd and correspondingVars into filterArrays
		for (int i=0; i<filtersToAdd.size(); i++)
			for (int j=0; j<TopologyMain.FILTER_LENGTH; j++)
				if (filtersToAdd.get(i)[j].equals(TopologyMain.DEFAULT_VAR))
					filtersToAdd.get(i)[j]=correspondingVars.get(i)[j];
	}

}
