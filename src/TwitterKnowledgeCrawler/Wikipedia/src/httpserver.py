from getmajor import *
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet import reactor
import socket
import threading
import Queue
import thread
import threading
import os
#HDFSLOC = "hdfs://10.3.10.21:9000"
#HOSTPORT = 12345

HDFSLOC = sys.argv[1]
HOSTPORT = sys.argv[2]



#Tweet ID which is assigned to the generated tweet. 
#Twitter assigns large values, so starting it from 1.
twid = 1
#Need to change it to avoid conflicts(say negative)

#list of requests
reqs = Queue.Queue()

#list of results
ress = Queue.Queue()

sem=threading.Lock()            #Triggered when new Tweet is added.


class FormPage(Resource):       #Rest API to add requests and see results(Mostly Testing)
    def render_GET(self, request):
        global sem,reqs,ress
        if request.prepath == ["getnexttweet"]:
            if ress.empty():
                return "None"
            else:
                return  str(ress.get())
        elif request.prepath == ["addtopic"]:
            reqs.put(request.args['topic'][0])
            return "Succesfully Added \""+ str(request.args['topic'][0]) + "\""
    def render_POST(self,request):
        try:
            pprint.pprint(request.content.read())
            tweet = str(request.content.getvalue())
            if request.prepath == ["addtopic"]:
                reqs.put(tweet)
                return "Succesfully Added  \"" + str(tweet) + "\""
        except:
            return "Adding Error"


def write_to_hdfs():        
#Writes all the results to hdfs /tmp/unclustered/tweets-(tweet number)
#Currently using system calls. Can be improved to use some python hadoop wrapper.
    written = 0
    towrite = ""
    inc = 0
    
    while(True):
        dat = ress.get()
        towrite += dat
        towrite += "\n"
        written += 1
        hdfsloc = HDFSLOC 
        if(written > 10):  
        #Writes the results after every 10 queries. 
        #So Each file contains the result of 10 queries.
            open("tweets/tweets-"+str(inc),"w").write(towrite)
            os.system("hadoop dfs -fs " +str(hdfsloc) + "-rmr /tmp/unclustered/tweets-"+str(inc))
            os.system("hadoop dfs -fs " + str(hdfsloc)+  "-copyFromLocal tweets/tweets-"+str(inc)+" /tmp/unclustered/tweets-"+str(inc))
            inc += 1
            written = 0
            towrite = ""



def insqueue(topic):
#Inserts the tweet generated corresponding to the topic into the ress queue. 
#Tweet has the format { 'id' : 'Tweet ID' , 'text' : 'Tweet'}
    global ress,twid
    print "inserting " + topic
    rets = get_wiki_tweet(topic,1) 
    while True:
        try:
            #One Tweet can cause many tweets to be inserted.
            #Keep insering until no more tweets can be formed from the given tweet.
            nxttwt = str(rets.next())   

            #Since twid is global, we don't want the tweet id's to be mixed up.
            #Hence next 2 operations should be atomic.
            #Can be solved by finding a better way to assign random tweet ids.
            sem.acquire()
            ress.put("{ \'id\' : \'%s\' , \'text\' : \'%s\'}"%(twid,nxttwt))
            twid += 1
            sem.release()
            print "added"
        except StopIteration:   #rets.next() will cause a StopIteration exception if get_wiki_tweet ends.
            break
            print "breaking"


def getrelated():
#Does some processing on the tweet and removes special chars etc.
    global reqs,twid
    while(1):
        topic = reqs.get()                      #First request from the queue.
        lstimp = get_most_important(topic,1)    #gets the most importatn words from the tweet.
        for i in range(len(lstimp)):
            thread.start_new_thread(insqueue,(lstimp[i],))#start a thread for generating a tweet for each important topic(Unigram).
        data=re.sub(r"[^a-zA-Z ]+", '', topic)  
        data=re.sub(" .. "," ",data)
        data=re.sub(" . "," ",data)
        data = data.split()
        data2 = []
        for i in data:
            if commontree.has_substring('$'+  str(i) + '$') == False:   #Remove stop words.
                data2.append(i)
        data = data2
        data = map((lambda x: x.lower()),data)
        for i in range(len(data)-1):
            thread.start_new_thread(insqueue,(data[i]+" "+data[i+1],))#use all continuous 2 words as a topic.


thread.start_new_thread(getrelated,())
thread.start_new_thread(write_to_hdfs,())
root = Resource()
root.putChild("getnexttweet", FormPage())
root.putChild("addtopic", FormPage())
factory = Site(root)
print "running..."
reactor.listenTCP(HOSTPORT, factory,interface='10.3.10.23')
reactor.run()

