# Sigmod-2018
Project based on Sigmod 2018 for the class Software Development for Information Systems of University of Athens. See the Final Report.pdf for more information.

# Abstract

The continuous advancement of technology used in the hardware domain has lead to the mass production of multi-core CPUs as well as to the decrease of RAM cost in terms of $/GB. In this project we demonstrate an efficient implementation of join operation in relational databases exploiting CPU parallelism and the large amount of available RAM in modern servers.

Our project, written in C language, is constructed in three parts which were then merged into a single one that complies with the instructions we were given. It features a hash-based radix partition join inspired by the join algorithms introduced in this [paper](https://15721.courses.cs.cmu.edu/spring2016/papers/balkesen-icde2013.pdf) and two parts of optimizations. The first one includes threading and parallelization and the second includes a precomputation analysis. The precomputation analysis is actually a statsitcal analysis which has as an output the optimal order of executing the given queries. This is done by optimizing our order having as a goal to minimize the cost function which is the accumulative amount of "middle" results. (the temporary number of tuples that occur from each join or filter of the query).

Additionally, it is worth mentioning that this project originates from the [SIGMOD Programming Contest 2018](https://db.in.tum.de/sigmod18contest/task.shtml) subject. Thus, we follow the task specifications of the contest and we also utilize the testing workloads provided to the contestants.
