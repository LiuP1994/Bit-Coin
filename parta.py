from mrjob.job import MRJob
import time

class parta(MRJob):
    def mapper(self, _, line):
        try:
            fields=line.split(",")
            timeepoch=fields[2]
            month=time.strftime("%Y %m",time.gmtime(int(timeepoch)))
            yield(month,1)
        except:
            pass

    def combiner(self, month, counts):
        yield(month,sum(counts))

    def reducer(self, month, counts):
        yield(month,sum(counts))

if __name__ == '__main__':
    # Lab4.JOBCONF= { 'mapreduce.job.reduces': '3' }
    parta.run()
