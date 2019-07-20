import pyspark
import re
import time
sc = pyspark.SparkContext()

#we will use this function later in our filter transformation
def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields)!=4:
            return False
        float(fields[1])
        float(fields[2])

        return True

    except:
        return False

def is_good_line1(line):
    try:
        fields = line.split(',')
        if len(fields)!=3:
            return False
        float(fields[2])
        return True

    except:
        return False

outline = sc.textFile("/data/bitcoin/voutsample")
inline=sc.textFile("/data/bitcoin/vinsample")
outlines = outline.filter(is_good_line)
inlines = inline.filter(is_good_line1)

inmap=inlines.map(lambda l: (l.split(',')[0].replace('\'',''),l.split(',')[1].replace('\'',''),float(l.split(',')[2])))
filted=outlines.filter(lambda l: (l.split(',')[3] == "{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"))
outhash=filted.map(lambda l: (l.split(',')[0].replace('\'','')))
join1=outhash.join(inmap)
# join11=join1.map(lambda l: (l.split(',')[1],l.split(',')[2]))
# outmap=outlines.map(lambda l: (l.split(',')[0],l.split(',')[1],l.split(',')[2],l.split(',')[3]))
# join2=join11.join(outmap)
# result=join2.filter(lambda l: (l.split(',')[1]==l.split(',')[4]))
# resultmap=result.map(lambda l:(l.split(',')[5],l.split(',')[3]))
# resultreduce = resultmap.reduceByKey(lambda a,b: a+b)




# top10 = features.takeOrdered(10, key = lambda x: -x[2])

# for record in top10:
#     print("{}: {};{}".format(record[0],record[1],record[2]))
# blocks = sc.textFile("/data/bitcoin/blocks.csv")
# good_blocks = blocks.filter( is_good_line )
# time_epoch = good_blocks.map(lambda b: b.split(',')[2] )
# year = time_epoch.map ( lambda t: (time.strftime("%y", time.gmtime(t)),1 ) )
# results = year.reduceByKey(lambda a,b: a+b)
join1.saveAsTextFile("join1")
