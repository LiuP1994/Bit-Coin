from mrjob.step import MRStep
from mrjob.job import MRJob

class partb_2(MRJob):

    sector_table = {}

    def mapper_join_init(self):
        with open("v_out.txt") as f:
            for line in f:
                fields = line.split(",")
                key = str(fields[0])
                val = fields[1]
                self.sector_table[key[1:]] = val

    def mapper(self, _, line):
        try:
            fields=line.split(",")
            txid=fields[0]
            tx_hash=fields[1]
            vout=fields[2]
            if txid in self.sector_table:
                yield('{},{},{}'.format(txid, tx_hash, vout),None)
            # yield(txid[1:],tx_hash)
        except:
            pass

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                        mapper=self.mapper)]

if __name__ == '__main__':
    partb_2.run()
