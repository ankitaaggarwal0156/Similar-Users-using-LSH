import sys
from pyspark import SparkContext
from collections import defaultdict
#import numpy as np
sc = SparkContext()
h= list()

def func(inp):
    data = list()
    a=list()
    for i in range(1, len(inp)):
        a.append(int(inp[0][1:]))
        a.append(int(inp[i]))
        data.append(a)
        a=list()        
    return data

def func1(lines):
    key = defaultdict(set)
    for i in range(0, len(lines)):
        for j in range(1, len(lines[i])):
            key[lines[i][0]].add(lines[i][j])
    return key.items()    
    
def hash1():
    a=list()
    for i in range(0,100):
        for j in range(0,20):
            a.append( (3*i + 13*j)%100)
        h.append(a)
        a=[]

def MinHash(lines):    
    sig = [100]*20
    for i in lines:
        for j in range(0,20): 
            if(sig[j]>h[i[1]][j] and sig[j]!=100):
                sig[j] = h[i[1]][j]
            if(sig[j]==100):
                sig[j] = h[i[1]][j]
    return sig    
    
    
def bandCompare(arr):
    key = defaultdict(list)
    for i in range(0, len(arr[0])):
        s=str(arr[0][i])+" "+str(arr[1][i])+" "+str(arr[2][i])+" "+str(arr[3][i])
        key[s].append(i+1)
    return key.values()      


def jaccard(lines,candidate):
    a=set()
    b=set()
    out=defaultdict()
    for i in range(0, len(candidate)):
        a = lines[candidate[i]-1][0][1]
        for j in range(i+1, len(candidate)):
            b = lines[candidate[j]-1][0][1]
            union1 = a.union(b)
            intersection1 = a.intersection(b)
            s = float(len(intersection1))/float(len(union1))
            out[(candidate[i],candidate[j])]=s
            out[(candidate[j],candidate[i])]=s
    return out.items()  
       
def Top5(result):
    count=0
    n=0
    out=defaultdict(list)
    for i in result:
        if(type(i)==int):
            n=i
        if(type(i)!=int):
            sortlist = sorted(i,key=lambda x: (-x[1], x[0]))  
    for k in range(0, len(sortlist)):
        if(count<5):
            out[n].append(sortlist[k][0])
            count=count+1
    return out.items()     


lines = sc.textFile(sys.argv[1])
lines = lines.map(lambda x: x.split(",")).map(lambda x: func(x))
hash1()
k= lines.map(MinHash)
#k1 = sc.parallelize(np.array(k.collect()).T)
k1 = list(k.collect())
kN = zip(*k1)
k2 = sc.parallelize(kN)
transformed = k2.zipWithIndex().groupBy(lambda (_, i): i / 4).map(lambda (_, list1): tuple([elem[0] for elem in list1]))
res = transformed.flatMap(bandCompare)
res = res.filter(lambda x:len(x)>1).map(lambda x: tuple(x))
res = res.distinct()
data = lines.map(func1).collect()
res = res.flatMap(lambda x: jaccard(data,x))
res = res.distinct().map(lambda x: (x[0][0], (x[0][1], x[1])))
res = res.groupByKey().map(lambda x: (x[0], list(x[1])))
res =res.flatMap(Top5).sortByKey()
filename = sys.argv[2]
f = open(filename,'w')
sort1=list()
out = res.collect()
for i in out:
    for j in i:
        if(type(j)==int):
            f.write("U%d:" %j)
        if(type(j)!=int):
            sort1 = sorted(j)
            for j in range(0, len(sort1)):
                if(j!=len(sort1)-1):
                    f.write("U%d," %(sort1[j]))
                else:
                    f.write("U%d\n" %(sort1[j]))
f.close()


