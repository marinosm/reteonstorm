#!/usr/bin/perl

# Expecting input to be sorted

$termId="";
$count=0;
while(<>){
	/CounterTerminal([0-9][0-9]*): partialCount=([0-9][0-9]*)/;
	$one=$1;
	$two=$2;
	if ($1 eq ""){
		print "CounterTerminal has no Id";
		exit 1;
	}
	if ($termId eq ""){
		$termId=$1
	}

	if ($termId eq $1){
		$count+=$2;
	}else{
		print "CounterTerminal$termId: count=$count\n";
		$termId=$1;
		$count=$2;
	}
}
print "CounterTerminal$termId: count=$count\n";
