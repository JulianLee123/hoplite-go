# Hoplite Go
## By Julian Lee and Brian Choi

We implemented a prototype of the task-based distributed system [Hoplite](https://arxiv.org/pdf/2002.05814.pdf) in Go, which has applications for fields including machine learning (e.g. for reinforcement learning). This system allows workers nodes to run tasks in parallel and communicate results via RPC only when necessary. Our implementation has following benefits:
* Facilitates running tasks in parallel across nodes when possible.
* Tasks can be submitted in any order, even when subsequent tasks rely on the output objects of previous tasks. Hoplite returns promises in the form of Object IDs for current tasks, and nodes requiring a promised object will download this object from the appropriate node once the task is complete.
	* Worker nodes can find the source node holding the promised object using one RPC call through a sharded object directory. This reduces any potential communication bottleneck.

Note that this framework also allows the nodes to be configured so that n nodes can crash without losing previous results (preliminary fault tolerance). This is done through providing a shard map where every shard is replicated n times, and having a worker node successfully broadcast n-1 copies of every produced object. 

## Details 

This markdown covers a high level overview. Additional design considerations and details can be found in our [writeup](hoplite_go.pdf). 

Worker nodes expose 2 main RPC calls: ScheduleTask allows tasks to be asynchronously scheduled, GetTaskAns synchronously retreives the result from a previously submitted task.

Each node has 3 roles: a worker component that completes tasks, a local object directory to store objects fromt completed tasks, and a node in a sharded key value store Object Directory Service (ODS) to look up the node(s) in which specific objects are stored. Upon completing task X, the worker node saves output object Y to their local object directory and adds an ODS entry specifying that this node now contains the object Y. If the worker node needs other non-local objects to complete a task, it searches the ODS to find the appropriate host node and downloads the object from this node. The specific RPC calls between worker nodes is described in [Table 1](hoplite_go.pdf) of the writeup.

## Tests

To test our framework, we implement a basic scheduler to distribute tasks between nodes.

The system was tested using 1-5 nodes and different sharding schemes. We used the following 3 tasks for testing (object ID's refer to objects directly passed in along with the task as well as previously computed objects hosted on one or more nodes): 
* Task 1: takes in one object ID corresponding to a list of (large) integers. Removes non-prime numbers from this list and outputs the updated list.
* Task 2: takes in two object IDs, each corresponding to a list of (large integers). Element-wise multiplies the two lists together, removing extraneous element if list lengths don't match. 
* Task 3: given an argument list of objects to reduce, where each argument corresponds to an object storing an integer array, reduces objects to a single object through element-wise multiplication (generalization of Task 2). 

Note that the reduce task can involve $n$ objects that can be processed in any order. The reduce task streams in objects concurrently and reduces each object once it becomes available. Therefore, if one promised object takes much longer to become available than the others, this task can perform useful work and reduce the remaining provided objects in the meantime. 

Our main testing design was based on concurrently scheduling a large number of tasks, where approximately half of the tasks (assigned Task 2 or 3) relied on object futures to be generated from the other half of tasks (assigned Task 1). Then, we concurrently request the results from each task and do a quick correctness verification. We then send delete requests for all the created objects, wait for half a second for the objects to be deleted, and check to make sure we can no longer retreive the objects using $GetTaskAns$. This main test evaluates Hoplite's ability to execute concurrent tasks including tasks with promises on a 1-5 node system, and it also highlights Hoplite's ease of use since all the tasks as well as the $GetTaskAns$ requests can be sent concurrently whether or not any given task has been processed yet. 

## Running Code

We have a suite of unit tests and integration tests as well as a couple benchmarks. These can be run within the hoplite_test folder using the commands below. Testing was done on Go v1.19.5.

``` 
go test -v
go test -bench=.
``` 