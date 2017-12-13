README.md  distributed_file_system
student number: 17305549
student name: Ying Jin
Introduction
1. this repository includes:
	1> locking server.py and locking client.py(this is independent with other three servers)
	2> distributed transparent file access.py
	3> directory server.py
	4> replication server.py
	5> clientcaching.py (this client can implement caching, but there is still basic functions that need to be modified, and may appear some mistakes when the client is running). 

2. run locking:
	python locking.py 8000 &
	python client_locking.py 8001 &

3. caching:
	1> load file and caching
	2> write the file while lock the file
	3> send changes to file system
	4> get file from cache
	5> rename the file

4. replication:
	1> load file
	2> this replicator manages files on every node
	3> write to one node
	4> replicate to other nodes

5. 