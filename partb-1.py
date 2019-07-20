from mrjob.job import MRJob

class partb(MRJob):
    def mapper(self, _, line):
        try:
            fields=line.split(",")
            publicKey=fields[3]
            a="{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}"
            if publicKey==a:
                yield(line,None)
        except:
            pass

if __name__ == '__main__':
    # Lab4.JOBCONF= { 'mapreduce.job.reduces': '3' }
    partb.run()
