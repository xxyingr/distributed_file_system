import threading
import time
import random

import imp,traceback

def loadFileDb(id,*args,**kwargs):
    
    try:
        fp, pathname, description = imp.find_module(id,__path__)
        assert fp
        return getattr(imp.load_module(id.replace(".",""),fp, pathname, description),"%sFileDb" % id)(*args,**kwargs)
    except Exception, err:
        print "error while loading filedb : %s" % err
        traceback.print_exc()
        return False
    

class Replicator(threading.Thread):
    '''
	This is the Replicator class.
	It manages files on every node.
	
	It takes a Filesystem instance in argument which is saved in self.fs.
	
    '''
    
    def __init__(self, fs):
        threading.Thread.__init__(self)
        self.fs = fs
        self.stopnow = False
        
    
    def shutdown(self):
        '''
        Shutdown the server by setting the variable self.stopnow to True.
        It breaks the while which runs the Replicator.
        '''
        self.stopnow = True
    
    def run(self):
        '''
        to write
        '''
        
        self.filedb = self.fs.filedb

        while not self.stopnow:
            
            didSomething = False
            
            try:
                
                didSomething = self.replicateNextFile()
                
            except Exception,e:
                
                self.fs.error("When replicating : %s" % (e))
            
            
            #Sleep for one minute unless something happens..
            if not didSomething and self.fs.config["replicatorIdleTime"]>0:
                self.fs.debug("Idling for %s seconds max..." % self.fs.config["replicatorIdleTime"],"repl")
                [time.sleep(1) for i in range(self.fs.config["replicatorIdleTime"]) if not self.stopnow]
            
            else:
                time.sleep(self.fs.config["replicatorInterval"])
    
    
    
    def replicateNextFile(self):
        
        
        data = self.fs.selectFileToReplicate()
        
        self.fs.debug("File %s" % (data),"repl")
        
        if not data:
            return None
    
        #do we have enough space to download the file ?
        while self.fs.getFreeDisk()<data["size"]:
            
            maxKnFile = self.fs.filedb.getMaxKnInNode(self.fs.host)
            
            #no files to delete to make space!
            if not len(maxKnFile):
                return None
                
            maxKn = self.fs.filedb.getKn(maxKnFile[0])
            
            #no files with a high enough kn to make space!
            if maxKn<data["kn"]+1:
                return None
                
            self.fs.debug("Deleting file %s because it has kn=%s" % (
							maxKnFile[0],
							maxKn
						),"repl")
            
            self.fs.deleteFile(maxKnFile[0])
        
        
        downloaded = self.fs.downloadFile(data["file"],data["nodes"])

        return downloaded