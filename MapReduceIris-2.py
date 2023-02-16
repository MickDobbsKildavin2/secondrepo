#https://github.com/astan54321/PA3/blob/44628868dcc7f00feec9e4c4bdb9391558391ac7/problem2_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

DATA_RE = re.compile(r"[\w.-]+")
occupation="Farming-fishing"
occupation="Prof-specialty"
occupation="Exec-managerial"

class MRProb2_3(MRJob):
                    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_sepW_occupation,
                   reducer=self.reducer_get_avg)
        ]

        
    def mapper_get_sepW_occupation(self, _, line):
        # yield each petal width
        data = DATA_RE.findall(line)
        if occupation in data:
            sep_W = float(data[0])
            yield ("sepal width", sep_W)
        
    def reducer_get_avg(self, key, values):
        # get max of the petal widths
        size, total = 0, 0
        for val in values:
            size += 1
            total += val
            
        if occupation == "Iris-setosa":
            yield ("Farming-fishing average age is", round(total,1) / size)
        elif occupation=="Prof-specialty":
            yield ("Prof-specialty average age is", round(total,1) / size)
        else:
            yield ("Exec-managerial average age is", round(total,1) / size)
            
if __name__ == '__main__':
    MRProb2_3.run()        