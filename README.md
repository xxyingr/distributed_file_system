README.md  distributed_file_system
student number: 17305549
student name: Ying Jin
Introduction
1. this repository includes 
	locking server.py and locking client.py(this is independent with other three servers)
	distributed transparent file access.py
	directory server.py
	replication server.py
	client.py
	setup.py

2. how to setup:
	bash
	mkdir -p ClientFiles
	mkdir -p Database
	mkdir -p DirectoryServerFiles/8006
	mkdir -p DirectoryServerFiles/8009
	mkdir -p DirectoryServerFiles/8010

	python directory.py 8005 &
	python replication.py 8007 &
	python DTF.py 8006 &
	python DTF.py 8009 &
	python DTF.py 8010 &

	python setup.py

3. for locking
	python locking.py 8000 &
	python client_locking.py 8001 &