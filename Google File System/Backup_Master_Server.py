import socket
import threading
import os
import math
import pickle
import sys
import json 
import copy
import time

chunk_port=[6467,6468,6469,6470]
i=0
j=0

class MasterServer(object):
    
    def __init__(self, host, port):
        self.chunksize=2048
        self.uploaded_file=[]
        self.chunk_servers = {}
        self.file_map = {}
        self.size = 0
        self.num_chunk_servers = 4
        self.chunk_servers_info={}
        self.chunk_servers_chunk_count={}
        self.chunk_servers_chunk_count_present={}
        self.replica = {}
        self.chunkserver_down1=[]
        self.chunkserver_down2=[]
        self.chunkserver_down3=[]
        self.chunkserver_down4=[]
        self.fileinfo={}                        #{(filename, # of chunks)}
        self.file_table={}
        self.file_size_info={}
        self.active_list=[]
        self.filename = ''
        self.host = host
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.host, self.port))
        

    def numChunks(self,size):
        return int(math.ceil(size/self.chunksize))

    def chunkserverinfo(self,file_map):
    
        for i in file_map.keys():
            for j in range(len(file_map[i])):
                self.chunk_servers_info[file_map[i][j][1]].append((i,file_map[i][j][0]))
        # print(self.chunk_servers_info)
        # print(self.chunk_servers_chunk_count)
        i=0
        a=[len(self.chunk_servers_info[x]) for x in self.chunk_servers_info.keys() if x in self.active_list]
        flag=0
        b=copy.deepcopy(self.chunk_servers_info)
        maximum = max(a)
        while i < maximum:
            for p in self.active_list:
                if i < len(b[p]):
                    ml = list(self.chunk_servers_chunk_count_present.values())
                    ml.sort()
                    min1=min(ml)

                    for k in self.active_list:
                        # print("K",self.chunk_servers_.get(k))
                        if b[p][i] not in self.chunk_servers_info[k] and self.chunk_servers_chunk_count_present.get(k) == min1:
                            flag=1
                    if flag==0:
                        min1=ml[1]
                    flag=0

                    for k in self.active_list:
                        if b[p][i] not in self.chunk_servers_info[k] and self.chunk_servers_chunk_count_present.get(k) == min1:
                            # print(b[p][i],"??",min1)
                            self.chunk_servers_info[k].append(b[p][i])
                            self.chunk_servers_chunk_count[k] += 1
                            self.chunk_servers_chunk_count_present[k] += 1
                            flag=1
                        if flag:
                            flag=0
                            break
            i+=1
        # print("Updated one:")    
        # print(self.chunk_servers_info)
        # print(self.chunk_servers_chunk_count)
        i=0
        a1=[len(self.chunk_servers_info[x]) for x in self.chunk_servers_info.keys()]
        # print(a1)
        
        max1 = max(a1)
        while i < max1:
            p = 0
            for load in a1:
                #print(load)
                if i <= load-1:
                    try:
                        if self.chunk_servers_info[self.active_list[p]][i] not in self.replica.keys():
                            self.replica[self.chunk_servers_info[self.active_list[p]][i]] = [self.active_list[p]]
                        else:
                            if self.active_list[p] not in self.replica[self.chunk_servers_info[self.active_list[p]][i]]:
                                self.replica[self.chunk_servers_info[self.active_list[p]][i]].append(self.active_list[p])
                    except:
                        pass
                p+=1
            i+=1
        # print(self.replica)
        
        # result=str(self.replica)

    def allocChunks(self):
        i=0
        chunks=[]
        num_chunks = self.numChunks(self.size)
        # print("In alloc chunks",self.active_list)
        # print(self.chunk_servers_chunk_count)
        for j in range(0,num_chunks):
            mV = sorted(self.chunk_servers_chunk_count_present.items(), key=lambda x:x[1])
            minV=mV[0][0]
            # print(j+1,minV)
            # print(j+1,self.active_list[self.active_list.index(minV)])
            self.file_map[self.filename].append((j+1,self.active_list[self.active_list.index(minV)]))
            self.chunk_servers_chunk_count_present[minV] += 1
            self.chunk_servers_chunk_count[minV] += 1
            chunks.append((j+1,self.active_list[self.active_list.index(minV)]))
            i=(i+1)% len(self.active_list)
        # print(self.file_map)
        self.chunkserverinfo(self.file_map)
        return chunks


    def allocChunks_update(self,num_chunks_old,num_chunks_new):
        i=0
        chunks=self.file_table[self.filename]
        for j in range(num_chunks_old,num_chunks_new):
            mV = sorted(self.chunk_servers_chunk_count_present.items(), key=lambda x:x[1])
            minV=mV[0][0]
            # print(j+1,minV)
            # print(j+1,self.active_list[self.active_list.index(minV)])
            self.file_map[self.filename].append((j+1,self.active_list[self.active_list.index(minV)]))
            self.chunk_servers_chunk_count_present[minV] += 1
            self.chunk_servers_chunk_count[minV] += 1
            chunks.append((j+1,self.active_list[self.active_list.index(minV)]))
            i=(i+1)% len(self.active_list)
        # print(self.file_map)
        # print(chunks)
        self.file_table.update(self.file_map)
        self.chunkserverinfo(self.file_map)
        return chunks

    def write(self):
        if self.filename in self.file_map:
            pass
        self.file_map[self.filename] = []
        chunks = self.allocChunks()
        # print("Back1")
        num_chunks = self.numChunks(self.size)
        self.fileinfo[self.filename]=num_chunks
        return chunks
    
    def write_update(self):
        if self.filename in self.file_map:
            pass
        self.file_map[self.filename] = []
        
        # # print("Back1")
        num_chunks_old = len(self.file_table[self.filename])
        num_chunks = self.numChunks(self.size)
        # print("Old chunks count : ",num_chunks_old)
        # print("new chunks count : ",num_chunks)
        chunks = self.allocChunks_update(num_chunks_old,num_chunks)
        self.fileinfo[self.filename]=num_chunks
        return chunks


    def upload(self):
        chunks = self.write()
        return chunks

    def do_the_deed(self,flag):
        
        if flag==1:
            #print((self.chunkserver_down1))
            for i in self.chunkserver_down1:
                if i[1]==1:
                    secondary=self.replica[i[0]][1]
                elif i[1]==0:
                    secondary=self.replica[i[0]][0]
                   
                ind=self.active_list.index(secondary)
                tertiary=self.active_list[(ind+1)%len(self.active_list)]
                #print(chunk_port[tertiary-1])
                self.chunk_servers_chunk_count[tertiary]+=1
                
                
                s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s1.connect((socket.gethostbyname('localhost'),chunk_port[secondary-1]))
           
                # stri="swap"+":"+str(i[0])+":"+str(ind)+":"+str(tertiary)
                stri="swap"+":"+str(i[0])+":"+str(secondary)+":"+str(chunk_port[tertiary-1])
                s1.send(bytes(stri,"utf-8"))
                s1.close() 
                self.replica[i[0]][0]=secondary 
                self.replica[i[0]][1]=tertiary
                # print(self.replica[i[0]][0])
                # print(self.replica[i[0]][1])

        if flag==2:
            #print(self.chunkserver_down2)
            for i in self.chunkserver_down2:
                if i[1]==1:
                    secondary=self.replica[i[0]][1]
                elif i[1]==0:
                    secondary=self.replica[i[0]][0]
                   
                ind=self.active_list.index(secondary)
                tertiary=self.active_list[(ind+1)%len(self.active_list)]
                # print(chunk_port[tertiary-1])
                self.chunk_servers_chunk_count[tertiary]+=1
                
                
                s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s1.connect((socket.gethostbyname('localhost'),chunk_port[secondary-1]))
              
                # stri="swap"+":"+str(i[0])+":"+str(ind)+":"+str(tertiary)
                stri="swap"+":"+str(i[0])+":"+str(secondary)+":"+str(chunk_port[tertiary-1])
                s1.send(bytes(stri,"utf-8"))
                s1.close() 
                self.replica[i[0]][0]=secondary 
                self.replica[i[0]][1]=tertiary   

        if flag==3:
            #print(self.chunkserver_down3)
           
            
            for i in self.chunkserver_down3:
                if i[1]==1:
                    secondary=self.replica[i[0]][1]
                elif i[1]==0:
                    secondary=self.replica[i[0]][0]
                   
                ind=self.active_list.index(secondary)
                
                tertiary=self.active_list[(ind+1)%len(self.active_list)]
                # print(chunk_port[tertiary-1])
                self.chunk_servers_chunk_count[tertiary]+=1
                s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s1.connect((socket.gethostbyname('localhost'),chunk_port[secondary-1]))            
                    
                stri="swap"+":"+str(i[0])+":"+str(secondary)+":"+str(chunk_port[tertiary-1])
                s1.send(bytes(stri,"utf-8"))
                s1.close() 
                self.replica[i[0]][0]=secondary 
                self.replica[i[0]][1]=tertiary  

        if flag==4:
            #print(self.chunkserver_down4)
            
            
            for i in self.chunkserver_down4:
                if i[1]==1:
                    secondary=self.replica[i[0]][1]
                    
                    
                elif i[1]==0:
                    secondary=self.replica[i[0]][0]
                   
                ind=self.active_list.index(secondary)
                
                tertiary=self.active_list[(ind+1)%len(self.active_list)]
                # print(chunk_port[tertiary-1])
                self.chunk_servers_chunk_count[tertiary]+=1
                
                
                s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
                s1.connect((socket.gethostbyname('localhost'),chunk_port[secondary-1]))
              
                # stri="swap"+":"+str(i[0])+":"+str(ind)+":"+str(tertiary)
                stri="swap"+":"+str(i[0])+":"+str(secondary)+":"+str(chunk_port[tertiary-1])
                s1.send(bytes(stri,"utf-8"))
                s1.close() 
                self.replica[i[0]][0]=secondary 
                self.replica[i[0]][1]=tertiary      

        print("The active file list inside the dothedeeds",end="->")
        print(self.active_list)


    def heartbeat(self):
        while True:

            time.sleep(6)
            
            s1=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s2=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s3=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            s4=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
            flag=[0,0,0,0]
            self.active_list=[]
            try:
                s1.connect((socket.gethostbyname('localhost'),6467))
                if 1 not in self.active_list:
                    self.active_list.append(1)
                flag[0]=0
                if 1 not in self.chunk_servers_chunk_count_present:
                    self.chunk_servers_chunk_count_present[1]=0
            except:
                if 1 in self.chunk_servers_chunk_count_present:
                    del self.chunk_servers_chunk_count_present[1]
                self.chunkserver_down1=[]
                keys_rep=list(self.replica.keys())
                value_rep=list(self.replica.values())
                for i in range(len(value_rep)):
                    if(value_rep[i][0]==1):
                        self.chunkserver_down1.append((keys_rep[i],1))
                    elif(value_rep[i][1]==1):
                        self.chunkserver_down1.append((keys_rep[i],0))

                flag[0]=1 

            try:
                s2.connect((socket.gethostbyname('localhost'),6468))
                if 2 not in self.active_list:
                    self.active_list.append(2)
                flag[1]=0
                if 2 not in self.chunk_servers_chunk_count_present:
                    self.chunk_servers_chunk_count_present[2]=0
            except:
                if 2 in self.chunk_servers_chunk_count_present:
                    del self.chunk_servers_chunk_count_present[2]
                self.chunkserver_down2=[]
                #print("Error from chunkserver2")
            

                keys_rep=list(self.replica.keys())
                value_rep=list(self.replica.values())
            
                for i in range(len(value_rep)):
                    if(value_rep[i][0]==2):
                        self.chunkserver_down2.append((keys_rep[i],1))
                    elif(value_rep[i][1]==2):
                        self.chunkserver_down2.append((keys_rep[i],0))
                
            
                flag[1]=1
            try:
                s3.connect((socket.gethostbyname('localhost'),6469))
                if 3 not in self.active_list:
                    self.active_list.append(3)
                flag[2]=0
                if 3 not in self.chunk_servers_chunk_count_present:
                    self.chunk_servers_chunk_count_present[3]=0
            except:
                if 3 in self.chunk_servers_chunk_count_present:
                    del self.chunk_servers_chunk_count_present[3]
                self.chunkserver_down3=[]
                #print("Error from chunkserver3")

                keys_rep=list(self.replica.keys())
                value_rep=list(self.replica.values())
            
                for i in range(len(value_rep)):
                    if(value_rep[i][0]==3):
                        self.chunkserver_down3.append((keys_rep[i],1))
                    elif(value_rep[i][1]==3):
                        self.chunkserver_down3.append((keys_rep[i],0))
            
                flag[2]=1
                
            try:
                s4.connect((socket.gethostbyname('localhost'),6470))
                if 4 not in self.active_list:
                    self.active_list.append(4)
                flag[3]=0
                if 4 not in self.chunk_servers_chunk_count_present:
                    self.chunk_servers_chunk_count_present[4]=0
            except:
                if 4 in self.chunk_servers_chunk_count_present:
                    del self.chunk_servers_chunk_count_present[4]
                self.chunkserver_down4=[]

                keys_rep=list(self.replica.keys())
                value_rep=list(self.replica.values())
            #print(keys_rep)
            #print(value_rep)
                for i in range(len(value_rep)):
                    if(value_rep[i][0]==4):
                        self.chunkserver_down4.append((keys_rep[i],1))
                    elif(value_rep[i][1]==4):
                        self.chunkserver_down4.append((keys_rep[i],0))
            
                flag[3]=1

            # print(flag)

            if flag[0]==1:
                self.do_the_deed(1)
            if flag[1]==1:
                self.do_the_deed(2)
            if flag[2]==1:
                self.do_the_deed(3)
            if flag[3]==1:
                self.do_the_deed(4)
                           
            print("The active list is",end=" ")
            print("AL",self.active_list)
            # for i in self.active_list:
            #     self.chunk_servers_info[i]=[]
            # print("Chunk server info",self.chunk_servers_info)                
            # print("count present",self.chunk_servers_chunk_count_present)
            s1.close()
            s2.close()
            s3.close()
            s4.close()
   
    def listen(self):
            global i
            if i<1:
                thread=threading.Thread(target=self.heartbeat,args=())
                thread.start()
                i+=1

            self.sock.listen(5)
            for i in range(1,5):
                self.chunk_servers_chunk_count[i]=0
            while True:
                client, address = self.sock.accept()
                client.settimeout(60)
                threading.Thread(target = self.commonlisten,args = (client,address)).start()
            
    def listenToClient(self, client, address,filename,size):
        self.filename=filename
        self.size=int(size)

        if self.filename not in self.file_size_info.keys():
            self.file_size_info[filename]=self.size

        # print(self.filename)
        if filename not in self.uploaded_file:
            self.all_file_info[filename]=0
            # print(self.all_file_info)
            self.uploaded_file.append(filename)
            chunks=self.upload()

            result=str(self.replica)
            names=str(self.uploaded_file)
            names1=str(self.fileinfo)
            names2=str(self.all_file_info)
            result=result+"\n"+names+"\n"+names1+"\n"+names2
            with open("log_file.txt","w") as f:
                f.write(result)
            
            self.file_map={}
            # print("Back3")
            data=pickle.dumps(chunks)
            # print("CS_count",self.chunk_servers_chunk_count)
            self.chunk_servers_info={}
            self.chunk_servers_chunk_count_present={}
            # client.send(b'Upload')
            aa="Upload"
            msg=pickle.dumps(aa)
            client.send(msg)
            time.sleep(0.3)
            client.send(data)
        else:
            self.chunk_servers_info={}
            self.chunk_servers_chunk_count_present={}
            aa="Present"
            msg=pickle.dumps(aa)
            client.send(msg)
    

    def listenToClientUpdate(self, client, address,filename,size):
        
        self.filename=filename
        self.size=int(size)

        if self.filename not in self.file_size_info.keys():
            self.file_size_info[filename]=self.size
        
        chunks = self.write_update()
        
        result=str(self.replica)
        names=str(self.uploaded_file)
        names1=str(self.fileinfo)
        names2=str(self.all_file_info)
        result=result+"\n"+names+"\n"+names1+"\n"+names2
        with open("log_file.txt","w+") as f:
            f.write(result)
        
        self.file_map={}
        data=pickle.dumps(chunks)
        # # print("CS_count",self.chunk_servers_chunk_count)
        self.chunk_servers_info={}
        self.chunk_servers_chunk_count_present={}
        aa="update"
        msg=pickle.dumps(aa)
        client.send(msg)
        time.sleep(0.3)
        client.send(data)


    def listentoChunk(self,client,address,filename,chunkNo,recv_port):
        chunkNo=int(chunkNo)
        cport=chunk_port[self.replica[(filename,chunkNo)][1]-1]
        if cport==int(recv_port):
            cport=chunk_port[self.replica[(filename,chunkNo)][0]-1]
        cport=str(cport)
        client.send(bytes(cport,"utf-8"))


    def lease_timer(self,cal_time,filename):
        # print("Timer started!!!")
        start = time.time()
        time.clock()    
        elapsed = 0
        while elapsed < cal_time and self.all_file_info[filename]!=0:
            elapsed = time.time() - start
            time.sleep(1)
        self.all_file_info[filename]=0
        # print("File is free!!!")

    def commonlisten(self,client,address):
        global j
        if j==0:
           j=1
           if os.path.exists("log_file.txt"):        
                with open("log_file.txt","r") as f:
                    if os.stat("log_file.txt").st_size != 0:
                        data=f.readlines()
                        str1 = ''.join(data)
                        # print(str1)
                        data1,data2,data3,data4=str1.split("\n",3)
                        self.replica=eval(data1)
                        data2=data2.replace('\'', '') 
                        self.fileinfo=eval(data3)
                        self.all_file_info=eval(data4)
                        self.uploaded_file=data2.strip('][').split(', ')

        the_decision,one,two,three=client.recv(1024).decode("utf-8").split(":")

        if(the_decision=="chunkserver"):
            filename=one
            chunk__no=two
            recv_port=three
            self.listentoChunk(client,address,filename,chunk__no,recv_port)
        
        elif (the_decision=="client"):
            
            for i in self.active_list:
                self.chunk_servers_info[i]=[]
            
            if(one=="upload"):
                filename=two
                size=three
                self.listenToClient(client,address,filename,size)
            elif(one=="download"):
                filename=two
                count=self.fileinfo.get(filename)
                # print(count)
                res=[]
                while count > 0:
                    if self.replica[(filename,count)][0] in self.active_list:
                        res.append([count,self.replica[(filename,count)][0]])
                        count=count-1
                    else:
                        res.append([count,self.replica[(filename,count)][1]])
                        count=count-1
                res.reverse()    
                self.chunk_servers_info={}
                self.chunk_servers_chunk_count_present={}
                data=pickle.dumps(res)
                client.send(data)

            elif(one=="lease"):
                filename=two
                print("Lease for", filename)
                if self.all_file_info[filename]==0:
                    self.all_file_info[filename]=1
                    aa="Lease successful"
                    msg=pickle.dumps(aa)
                    client.send(msg)
                    self.lease_timer(30,filename)

                if self.all_file_info[filename]!=0:
                    aa="File unavailable for access at this time!!!"
                    msg=pickle.dumps(aa)
                    client.send(msg)
                    while self.all_file_info[filename]!=0:
                        pass
                    aa="Now you can access file: "+filename
                    # print(aa)
                
                    msg=pickle.dumps(aa)
                    client.send(msg)    

            elif(one=="unlease"):
                filename=two
                print("UnLease for", filename)
                self.all_file_info[filename]=0
                aa="UnLease successful"
                msg=pickle.dumps(aa)
                client.send(msg)
                
            elif(one=="listfiles"):
                file_list=list(self.all_file_info.keys())
                # print("FILES: ",file_list)
                msg=pickle.dumps(file_list)
                client.send(msg)

            elif one == "update":
                filename=two
                size=three
                self.listenToClientUpdate(client,address,filename,size)
                self.file_size_info[self.filename] = size    

if __name__ == "__main__":
    while True:
        
        try:
            port_num = 7083
            break
        except ValueError:
            pass
    print("Master Server Running")
    MasterServer('',port_num).listen()