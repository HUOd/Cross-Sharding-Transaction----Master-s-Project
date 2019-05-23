# Cross-Sharding-Transaction----Master-s-Project

The code starts from the MIT course 6.824 - Spring 2018's Lab's code

[link to 6.824](https://pdos.csail.mit.edu/6.824/index.html)


You could acquire the initial code use git clone:

    git clone git://g.csail.mit.edu/6.824-golabs-2018 6.824

so that you could know where did I get started.

Directories:

1.labgob: This package is for formating and parsing the persistence data in this system. I didn't modify this package.

2.labrpc: This package is a RPC library for this system, most of the communication are based on RPC call. I didn't modify this package.

3.linearizability: This package is for testing the linearizability of this system. I didn't modify this package.

4.raft: This package is the Raft algorithm library. I wrote the code in raft.go following the Lab 2 in 6.824.

5.shardkv: This package is the code for functionalities of shard key-value group. I wrote most of the code in shardkv/server.go and shardkv/client.go following the Lab4 in 6.824.

6.shardmaster: This package is the code for functionalities of shard master. I wrote most of the code in shardmaster/server.go and shardmaster/client.go following the Lab4 in 6.824.

7.transactionmanager: This package is the main contribution of this project. I wrote most of the code in transactionmanager/server.go and transactionmanager/client.go.
