import pyspark
import re
import time
sc = pyspark.SparkContext()
#we will use this function later in our filter transformation
def is_good_line1(line):
    try:
        fields = line.split(',')
        if len(fields)!=5:
            return False
        float(fields[2])
        return True
    except:
        return False

def is_good_line2(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False
        float(fields[1])
        return True
    except:
        return False

def extractday(time_epoch):
    day=time.strftime("%d",time.gmtime(int(time_epoch)))
    month=time.strftime("%m",time.gmtime(int(time_epoch)))
    year=time.strftime("%Y",time.gmtime(int(time_epoch)))
    return "{}-{}-{}".format(year,month,day)

lines1 = sc.textFile("/data/bitcoin/transactions.csv")
lines2 = sc.textFile("/data/bitcoin/vout.csv")
lines3 = sc.textFile("input/BTC-USD.csv")

clean_lines1 = lines1.filter(is_good_line1)
clean_lines2 = lines2.filter(is_good_line2)

map1=clean_lines1.map(lambda l: (l.split(',')[0],extractday(l.split(',')[2])))
map2=clean_lines2.map(lambda l: (l.split(',')[0],float(l.split(',')[1])))
coin= map1.join(map2)
coin1=coin.map(lambda l:(l[1][0],l[1][1]))
coinpd=coin1.reduceByKey(lambda a,b: a+b)

map3=clean_lines1.map(lambda l:(extractday(l.split(',')[2]),1))
tranpd = map3.reduceByKey(lambda a,b: a+b)
ctpd=coinpd.join(tranpd)

map3=lines3.map(lambda l:(l.split(',')[0],float(l.split(',')[1])))
ctppd=ctpd.join(map3)
sctppd=result.sortByKey()

sctppd.saveAsTextFile("out1")
# for record in top10:
#     print("{}: {};{}".format(record[0],record[1],record[2]))
