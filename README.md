reteonstorm
===========

The purpose of this project is to experiment with possible optimisations for a Storm implementation of the Rete algorithm

TopologyMain creates and runs a simple Rete Network as a Storm Topology (that works in local mode by reading input from a file).
It can take arguments to allow experimentation with the Storm Bolts parallelism etc.
Code in the "more" folder might prove useful later on (when Joins are added to the RETE network). 
Scripts in the scripts folder can produce well-formed input for the topology, run the topology with valid arguments, collect full output in a log-file and append a summary of the log-file to a summary-file. 
The log and resources folders are not included because of their size and because the contents can be reproduced from the scripts.
