import socket
import threading
import os
import math
import sys
import pickle
from ast import literal_eval as make_tuple #This module is used to convert a string into tuple...

class ChunkServer(object):
    
    def __init__(self, host, port, myChunkDir, filesystem):
        self.filesystem=filesystem
        self.myChunkDir=myChunkDir
        self.host = host
        self.port = port
        self.chunkserver1_info=[]
        self.chunkserver2_info=[]
        self.chunkserver3_info=[]
        self.chunkserver4_info=[] 
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))


    #listening to connections and after accepting makes a new thread for every connection
    def listen(self):
        self.sock.listen(5)
        while True:
            client, address = self.sock.accept()
            client.settimeout(60)
            threading.Thread(target = self.commonlisten,args = (client,address)).start()


    def connect_to_master(self,fname,chunk_id,filename):
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),7082))
        except:
            try:
                s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s.connect((socket.gethostbyname('localhost'),7083))
            except:
                print("No backup server is active...Try again later!!!")        
                sys.exit()
        
        filenameToCS=fname
        port=self.port
        # print("asdfg",port)
        fname="chunkserver:"+fname+":"+chunk_id+":"+str(port)
        s.send(bytes(fname,"utf-8"))
        cport=s.recv(2048).decode("utf-8")
        # print("AA",filename, chunk_id, cport)
        self.connectToChunk(cport,filenameToCS,chunk_id,filename)
    

    def connectToChunk(self,cport,filenameToCS,chunk_id,filename):
        try:
            s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s.connect((socket.gethostbyname('localhost'),int(cport)))
            fname="chunkserver:"+"dummy:"+filenameToCS+":"+chunk_id+":"+str(port_num)+":"
            fname=fname.ljust(400,'~')
            s.send(bytes(fname,"utf-8"))

            f1=open(filename,'rb')
            data=f1.read(2048)
            s.send(data)
        except:
            pass
        

    #The thread at work...accpeting each chunk from the client and storing in its directory

    def listenToChunk(self,client,address,one,two,three):
        # print(one,two,three)
        path=self.myChunkDir+"/"+str(one)+"_"+str(two)
        # print(path)
        with open(path, 'wb') as f:
            c_recv=client.recv(2048)
            # f.write(c_recv.decode("utf-8"))
            f.write(c_recv)


    def sendToClient(self,client,address,one,two,three):

        path=self.myChunkDir+"/"+str(three)+"_"+str(two)
        # print(path)
        with open(path, 'rb') as f:
            data=f.read(2048)
            client.send(data)


    def commonlisten(self,client,address):

        to_recv=client.recv(400).decode("utf-8")
        to_recv=to_recv.split(":")
        # to_recv=to_recv.split(":")

        if(to_recv[0]=="client"):
            
            if(to_recv[1]=="upload"):
                
                chunk_server_no=to_recv[2]
                chunk_id=to_recv[3]
                filenaming=to_recv[4]
                self.listenToClient(client,address,chunk_server_no,chunk_id,filenaming)
            
            if(to_recv[1]=="download"):
                # print(to_recv)
                chunk_server_no=to_recv[2]
                chunk_id=to_recv[3]
                filenaming=to_recv[4]
                self.sendToClient(client,address,chunk_server_no,chunk_id,filenaming)

        elif(to_recv[0]=="chunkserver"):
            # print(one,two,three)
            self.listenToChunk(client,address,to_recv[2],to_recv[3],to_recv[4])

        elif(to_recv[0]=="swap"):
            
            # print(to_recv[1])
            # print(to_recv[2])
            # print(to_recv[3])
            s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s1.connect((socket.gethostbyname('localhost'),int(to_recv[3])))
            to_recv[1]=make_tuple(to_recv[1])
            three=to_recv[1][0]
            two=to_recv[1][1]
            stri="downside"+":"+str(three)+":"+str(two)+":"
            stri=stri.ljust(400,'~')
            s1.send(bytes(stri,"utf-8"))
            
            
            path=self.myChunkDir+"/"+str(three)+"_"+str(two)
            with open(path, 'rb') as f:
                data=f.read(2048)
                s1.send(data)


            s1.close()
        elif(to_recv[0]=="downside"):
            three=to_recv[1]
            two=to_recv[2]
            path=self.myChunkDir+"/"+str(three)+"_"+str(two)
            with open(path,'wb') as f1:
                f1.write(client.recv(2048))

    def listenToClient(self, client, address, chunk_server_no, chunk_id, filenaming):

        if not os.access(self.filesystem, os.W_OK):
            os.makedirs(self.filesystem)
        
        filename = self.filesystem+"/"+filenaming+"_"+chunk_id
        # print(filename)
        with open(filename, 'wb') as f:
            chunks_recv=client.recv(2048)
            f.write(chunks_recv)

        if chunk_server_no=="1":
            self.chunkserver1_info.append((filenaming,chunk_id))
        elif chunk_server_no=="2":
            self.chunkserver2_info.append((filenaming,chunk_id))
        elif chunk_server_no=="3":
            self.chunkserver3_info.append((filenaming,chunk_id))
        elif chunk_server_no=="4":
            self.chunkserver4_info.append((filenaming,chunk_id))
        
       
        # if chunk_server_no == "1":
        #     print(self.chunkserver1_info)
        # elif chunk_server_no == "2":
        #     print(self.chunkserver2_info)
        # elif chunk_server_no == "3":
        #     print(self.chunkserver3_info)
        # elif chunk_server_no == "4":
        #     print(self.chunkserver4_info)
        self.connect_to_master(filenaming,chunk_id,filename)
        
        

if __name__ == "__main__":
    while True:
        try:
            # port_num = int(input("Enter the port number of the chunk_server"))
            port_num = int(sys.argv[1])
            if port_num==6467:
                filesystem = os.getcwd()+"/"+str(1)
                myChunkDir=filesystem
            if port_num==6468:
                filesystem = os.getcwd()+"/"+str(2)
                myChunkDir=filesystem
            if port_num==6469:
                filesystem = os.getcwd()+"/"+str(3)
                myChunkDir=filesystem
            if port_num==6470:
                filesystem = os.getcwd()+"/"+str(4)
                myChunkDir=filesystem
            break
        except ValueError:
            pass
    print("Chunk Server Running")
    ChunkServer('',port_num,myChunkDir, filesystem).listen()

    
