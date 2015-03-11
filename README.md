Grava
=====
Java (multi)-graph library for huge size graphs
This is a Java (multi)-graph library able to load huge graphs (~350M
edges and 50M nodes) into main memory in 10' on a normal machine with a
pretty good amount of memory (35G).

There are three basic (extendable) graph

* BaseMuligraph: can load up to 50M edges, but it is easier to use and to access, since everything is implemented with HashMap
* PartitionedMultigraph: can load up to three times the size of BaseMultigraph since it applies partitioning on the data
* BigMultigraph: can load very big graphs into main memory but it requires to specify files in a predefined format and already sorted (see below).

BigMultigraph usage
-------------------
It require a file that contains only long and with this syntax: 
SOURCE[SPACE]DEST[SPACE]LABEL
where source,dest and label are long and SPACE is ' '. You provide the name of
namefile-sin.graph namefile-sout.graph, sorted repectively on the second and the first column (use sort command in a linux like system to obtain them). 

This is still in an exeperimental version but fully working. Open an issue if you don't understand something. Good luck! 
