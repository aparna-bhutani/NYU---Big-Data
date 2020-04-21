import sys
import csv
from mrjob.job import MRJob, MRStep
import mapreduce as mr

class MRTask(MRJob):
    
    def mapper1(self,_, data):
        yield(data[3], data[0],1), float(data[4])
        
    def reducer1(self, data, ones):
        yield (data[0], data[2]), sum(ones)


    def reducer2(self, total, ones):
        yield total[0], len(ones), sum(ones) 

    def steps(self):
        return[
            MRStep (mapper = self.mapper1,
                   reducer = self.reducer1),
            MRStep (reducer = self.reducer2),

        ]

if __name__ == '__main__':
    with open(sys.argv[1], 'r') as fi:
        reader = csv.reader(fi)
        output = list(mr.runJob(enumerate(reader), MRTask(args = [])))
        
    with open(sys.argv[2], 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(output)


