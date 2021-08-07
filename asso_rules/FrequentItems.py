#! /usr/bin/python3
# FrequentItems.py

import sys
from FrequentKItemsetJob import FrequentKItemsetJob
from itertools import combinations
from bitarray import bitarray
transaction_count = None
k_minus_one_itemsets = {}

def runjob(job,iteration):
     with job.make_runner() as runner:
          print("Running iteration {}\n".format(job.options.iteration))
          runner.run()
          if iteration == 1:
               global transaction_count
               counters = runner.counters()
               transaction_count = counters[0]['association_rules']['transaction_count']
          for key, value in job.parse_output(runner.cat_output()):
               if iteration >= job.options.k - 1:
                    if value / transaction_count >= float(job.options.s):
                         yield set(key), value / transaction_count
          if job.options.type==1:
             for key, value in job.parse_output(runner.cat_output()):
                   support = value / transaction_count
                   if support >= float(job.options.s):
#                     frequent=1
                      bitmap.append(True)
                      print('Frequent {}\tcount = {}\tsupport = {}'.format(key,value,round(support,3)))
#                     frequent=0
                   else:
                      bitmap.append(False)
          fh.close()


if __name__ == '__main__':
     args = sys.argv[1:]

     job = FrequentKItemsetJob(args + ['-iteration','1'])
     k = job.options.k
     global bitmap
     bitmap=bitarray(1)
     for i in range(1,k+1):
          job = FrequentKItemsetJob(args = args+['-iteration',str(i),'-bit',bitmap.to01()])
          results = runjob(job,iteration=i)
          if i == job.options.k - 1:
               for result in results:
                    k_minus_one_itemsets[frozenset(result[0])] = result[1]
               #print(k_minus_one_itemsets)
          elif i == job.options.k:
               #print(k_minus_one_itemsets)
               for result in results:
                    lhs_items = combinations(result[0], job.options.k-1)
                    try:
                         for lhs_item in lhs_items:
                            confidence = list(result)[1] / k_minus_one_itemsets[frozenset(lhs_item)]
                            support = result[1]
                         
                            if confidence > job.options.c:
                              rhs_item = next(iter(set(result[0]).difference(lhs_item)),'')
                              output_string = "{} ---> {}. support = {}, confidence = {}"
                              print(output_string.format(
                                  ",".join(lhs_item), 
                                  rhs_item, 
                                  support, 
                                  confidence))
                    except:
                             pass

          else:
               fh = open(job.options.f + ".txt", "w")
               for result in results:
                    key = result[0]
                    value = result[1]
                    if value / transaction_count >= float(job.options.s):
                         fh.write('{}\n'.format(key))
                         if iteration == job.options.k:
                              if job.options.k == 1:
                                   print(key)
                              else:
                                   print(set(key))
               fh.close()
