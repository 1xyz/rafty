# Rafty - Simulation of Raft processes 

This is an experimental project, primarily to understand the usage of the etcd-raft library. 

To summarize, rafty is a simulation of multiple Raft nodes in a single process. The different raftyNodes communicate with each other by calling a recvRPC on the nodes.  

Here are some useful references that I have collected so far, that has helped me.

References
1) https://www.reddit.com/r/golang/comments/3dzprs/using_the_etcd_raft_to_build_your_first_cluster/
2) https://otm.github.io/2015/05/raft-a-first-implementation/
3) https://github.com/otm/raft-part-1/blob/3ec341756c99d0f866a1e39099ae780fb662ab37/raft-part-1.go
4) https://godoc.org/github.com/coreos/etcd/raft
5) https://youtu.be/c2RyuTyVHxE?t=829
6) https://www.reddit.com/r/golang/comments/9emakk/should_i_use_etcd_for_this_project_or_write_my/
