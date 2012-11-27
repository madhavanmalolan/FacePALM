import nltk
import sys
from nltk.collocations import *
from nltk.corpus import stopwords
import WikiExtractor as we
import urllib
import urllib2
import json
import re
import pprint
from suffix_tree import *
import json
import redis

#Length of the tweet.
TWEET_LENGTH = 10

#Using Redis Distributed datastore to store generated tweets so that it doesn't have to generate the tweet again and again.
#Currently only on localhost.
#If need to make distributed, change here.
dbstore = redis.Redis("localhost")


#set of stop words.
common = set (stopwords.words('english'))

#suffix tree of set of stopwords.
commontree = SuffixTree('$' + str('$'.join(list(common))) + '$')


def get_sing(word,tag):
#Converts the word to a singular.
    if word[-1] == 's' and tag == "NNS":
        return word[:-1]
    return word



def singularize_plural_noun(chunk):
#Creates the list of (words,tags).
#Returns a list coontaining only singular words.
  chunk = nltk.pos_tag(chunk)
  res2 = []
  forb = ['DT','CC']
  for (word,tag) in chunk:
    ok = True
    for suffix in forb:
     if(tag.endswith(suffix)):
        ok = False
        break
    if ok:
        res2.append((word,tag))
  res = map((lambda x:  get_sing(x[0],x[1])),res2)
  return res



def get_most_important(data,UNIGRAM=1):
#Returns the most important words in a (wiki) page.
#Uses Unigram if UNIGRAM = 1, else uses BIGRAM.
#By default, it is unigram.
    data=re.sub(r"[^a-zA-Z ]+", '', data)
    data=re.sub(" .. "," ",data)
    data=re.sub(" . "," ",data)
    data = data.split()
    data2 = []
    for i in data:                  #Remove stop words.
        if commontree.has_substring('$'+  str(i) + '$') == False:
            data2.append(i)
    data = data2
    data = map((lambda x: x.lower()),data)
    #data = nltk.word_tokenize(data)
    data = singularize_plural_noun(data) #Tokenization done in this function.
    if(UNIGRAM == 1):
        lst = {}
        for i in data:
            if(lst.get(i)==None):
                lst[i] = 0 
            lst[i] += 1
        nlst = map((lambda x: (x[1],x[0])), lst.items())
        nlst.sort()                     #Take top occuring words.
        nlst.reverse()
        return map((lambda x: x[1]),nlst[:TWEET_LENGTH])    
    bigram_measures = nltk.collocations.BigramAssocMeasures()
    finder = BigramCollocationFinder.from_words(data)
    finder.apply_freq_filter(3)                 #uses nltk's bigram to get most significant bigrams.
    return map((lambda x: str(x[0])+" " + str(x[1])),finder.nbest(bigram_measures.pmi, TWEET_LENGTH))




def get_title(squery):
#Gets the wiki page for the title squery.
    url = "http://en.wikipedia.org/w/api.php?action=query&prop=revisions&rvprop=content&format=json&"+str(urllib.urlencode({"titles":squery}))
    while True:
        try:
            dat = urllib.urlopen(url).read()
            dat = json.loads(dat)
            break
        except ValueError:      #Proxy might reject request.
            print "Retrying"
            pass
    kys = dat['query']['pages'].keys()[0]
    dat = dat['query']['pages'][kys]['revisions'][-1]['*']
    redir = re.findall("#REDIRECT \[\[(.*?)\]\]",dat,re.IGNORECASE) #Handle Redirection.
    if(len(redir)!=0):
        return get_title(redir[0])
    print "Retrieved " + str(squery)
    dat2 = we.clean(dat)
    return dat2             #TWEET_LENGTH word Tweet.

    
    
def get_wiki_tweet(squery,unibifl):
#Returns the TWEET_LENGTH word tweet generated.
#Appends the title of the search to the beginning of the result tweet.
    url = "http://en.wikipedia.org/w/api.php?action=opensearch&"+urllib.urlencode({"search":str(squery)})
    ret = []
    while True:
        try:
            titles = urllib.urlopen(url).read() #List of titles similar to squery.
            titles = json.loads(titles)[1]
            break
        except ValueError:                      #Proxy might reject request
            print "Retrying"
            pass
    print titles
    for i in titles:                            #As and when tweets are generated, yield them.
        if(dbstore.get(i) != None):
            yield dbstore.get(i)
            continue
        imp = get_most_important(get_title(i),unibifl)
        imp.insert(0,i)
        dbstore.set(i, " ".join(imp))
        yield dbstore.get(i)        
