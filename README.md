README.md  distributed_file_system
student number: 17305549
student name: Ying Jin
Introduction
1. this repository includes 
	locking server.py and locking client.py(this is independent with other three servers)
	distributed transparent file access.py
	directory server.py
		this server is created like:
		
			import web
			import directory
			url = ('(/.*)','directory',)
			if __name__=='__main__'
	
	replication server.py
	client.py (this client can implement caching, but there is still basic functions that need to be updeated, and may appear some mistakes when the client is running)

2. how to setup:
	


3. for locking
	python locking.py 8000 &
	python client_locking.py 8001 &

4. file servers are based on web.py