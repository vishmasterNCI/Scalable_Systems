#! /usr/bin/python3
# FrequentKItemsetJob.py

from mrjob.job import MRJob, MRStep
from itertools import combinations
from bitarray import bitarray

class FrequentKItemsetJob(MRJob):

    frequent_items = []
    bitmap=bitarray()
    def configure_args(self):
        super(FrequentKItemsetJob, self).configure_args()
        self.add_passthru_arg('-iteration', type=int, help="The current iteration. Not used as a command line argument")
        self.add_passthru_arg('-bit', type=str, help="The current bitmap. Not used as a command line argument")
        self.add_passthru_arg('--b', type=int, default=10, help="Specify the number of buckets")
        self.add_passthru_arg('--k', type=int, default=3, help="Specify the maximum size of itemsets to find")
        self.add_passthru_arg('--s', type=float, help="Specify the minimum support threshold")
        self.add_passthru_arg('--c', type=float, default=0, help="Specify the minimum confidence threshold")
        self.add_file_arg('--f', default='frequent.txt', help="Specify the name of the file used to store frequent itemsets")
        self.add_passthru_arg('--type',type=int,default=0, help="The current algorithm. Not used as a command line argument")
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_get_items_init,
                   mapper=self.mapper_get_items,
                   combiner=self.combiner_count_items,
                   reducer=self.reducer_total_items)
        ]

    def mapper_get_items_init(self):
        if int(self.options.iteration) > 1:
            with open(self.options.f,'r') as fh:
                self.frequent_items = set(fh.read().splitlines())
        else:
            self.frequent_items = {}

    def mapper_get_items(self, _, line):
        lineitems = line.split(",")
        if int(self.options.iteration) == 1:
            self.increment_counter("association_rules", 'transaction_count', 1)
            for item in lineitems:
                yield item.strip(), 1
        else:
          if int(self.options.type)==1:
            itemsets = combinations(lineitems, self.options.iteration)
            frequent_itemsets = filter(lambda x: set(x) not in self.frequent_items, itemsets)
            for itemset in frequent_itemsets:
                yield itemset, 1
            pairs = combinations(lineitems, self.options.iteration)#,combinations(lineitems,2))
            for pair in pairs:
                        pair_string = str([''.join(item) for item in pair])
                        pair_hash = hash(pair_string)
                        bucket = pair_hash % self.options.b
                        yield "Bucket {}".format(bucket), 1
            if  int(self.options.iteration) ==3:
                itemsets = combinations(lineitems, self.options.iteration)
                frequent_itemsets = filter(lambda x: set(x) not in self.frequent_items, itemsets)
                for pair in pairs:
                  pair_string = str([''.join(item) for item in pair])
                  pair_hash = hash(pair_string)
                  bucket = pair_hash % self.options.b*2
                  self.bitmap=bitmap
                  for itemset in frequent_itemsets:
                    if self.bitmap[bucket]==1:
                       yield itemset, 1

          else:
            itemsets = combinations(lineitems, self.options.iteration)
            frequent_itemsets = filter(lambda x: set(x) not in self.frequent_items, itemsets)
            for itemset in frequent_itemsets:
                yield itemset, 1
            #pairs = combinations(lineitems, self.options.iteration)#,combinations(lineitems,2))
            #for pair in pairs:
            #            pair_string = str([''.join(item) for item in pair])
            #            pair_hash = hash(pair_string)
            #            bucket = pair_hash % self.options.b
            #            yield "Bucket {}".format(bucket), 1
            #if  int(self.options.iteration) ==3:
            #    itemsets = combinations(lineitems, self.options.iteration)
            #    frequent_itemsets = filter(lambda x: set(x) not in self.frequent_items, itemsets)
            #    for pair in pairs:
            #      pair_string = str([''.join(item) for item in pair])
            #      pair_hash = hash(pair_string)
            #      bucket = pair_hash % self.options.b*2
            #      self.bitmap=bitmap
            #      for itemset in frequent_itemsets:
             #       if self.bitmap[bucket]==1:
              #         yield itemset, 1




    def combiner_count_items(self, item, counts):
        yield item, sum(counts)

    def reducer_total_items(self, item, counts):
        yield item, sum(counts)


if __name__ == '__main__':
    FrequentKItemsetJob.run()
