from mrjob.step import MRStep
from mrjob.job import MRJob

class partb_3(MRJob):

    sector_table = {}

    def mapper_join_init(self):
        with open("v_in.txt") as f:
            for line in f:
                fields=line.split("\"")
                code=fields[1].split(",")
                tx_hash = code[1]
                vout = code[2]
                self.sector_table[tx_hash] = vout

    def mapper_a(self, _, line):
        try:
            fields=line.split(",")
            hash=fields[0]
            value=float(fields[1])
            n=fields[2]
            publicKey=fields[3]
            if hash in self.sector_table:
                if n==self.sector_table[hash]:
                    yield(publicKey,value)
        except:
            pass

    def reducer_a(self, publicKey, value):
        yield(publicKey,sum(value))

    def mapper_b(self, publicKey, sum):
        yield(None,(publicKey,sum))

    def reducer_b(self, _, details):
        sorted_values = sorted(details, reverse=True, key=lambda details : float(details[1]))
        k=0
        for value in sorted_values:
            k+=1
            if k<11:
                yield(value[0],value[1])

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper_a,
                        reducer=self.reducer_a),
                MRStep(mapper=self.mapper_b,
                        reducer=self.reducer_b)]

if __name__ == '__main__':
    partb_3.run()
