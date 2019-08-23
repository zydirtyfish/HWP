#!/usr/bin/python
# -*- coding: UTF-8 -*-

import numpy as np
from LRUCache import LRUCache
import time

feature_num = 14
#cache_size = 1310720 # 200 GB/Node
#cache_size = 327680 # 50 GB/Node
cache_size = 655360 # 100 GB/Node
#memory_size = 26214 # 4 GB/Node
memory_size = 13107 # 2 GB/Node
#memory_size = 6554 # 1 GB/Node
tablet = {}

def main():
	file_dir = '/cbs_trace1/sample_6/'
	
	MemoryCache = LRUCache(memory_size)
	SSDCache = LRUCache(cache_size)


	current_time = 1538323200
	for i in range(1,31):

		ssd_hit_num = 0
		memory_hit_num = 0
		total_read = 0
	
		total_write = 0
		memory_write_num = 0
		ssd_write_num = 0
		hdd_write_num = 0

		print('----------------------------------')
		cudate = time.strftime("%Y-%m-%d", time.localtime(current_time))
		print(cudate)
		current_time += 86400
		print('----------------------------------')
		file_name = file_dir + str(i)
		f = open(file_name,"r")
		print(file_name)
		for line in f.readlines():
			io_record = line.split(",")

			#update tablet
			key = io_record[4][:-2]+','+ str(int(io_record[1]) >> 11)
			if key in tablet:
				a = tablet[key]
				a[0] = a[0] + 1
			else:
				a = [1 for i in range(0,feature_num)]
				tablet[key] = a

			block = io_record[4][:-2]+','+ str(int(io_record[1]) >> 6)
			if int(io_record[3]) == 1:
				#write
				total_write += 1
				ssd_write_num += 1
				ssd_evict = SSDCache.set(block,1)
				if ssd_evict != None:
					hdd_write_num += 1

			else:
				#read
				total_read += 1
				if SSDCache.get(block) != None:
					#命中
					ssd_hit_num += 1
				else:
					evict = SSDCache.set(block,1)
					ssd_write_num += 1
					if evict != None:
						hdd_write_num += 1

		print("SSD footprint : %.2f %%"%(len(SSDCache.cache) * 100 / cache_size))
		print("memory_write : %.2f %%"%(memory_write_num * 100 / total_write))
		#need to write ssd log once when write to memory once
		print("ssd_write : %.2f %%"%((ssd_write_num+memory_write_num) * 100 / total_write))
		print("hdd_write : %.2f %%"%(hdd_write_num * 100 / total_write))
		print("Memory Hit Ratio : %.2f %%"%(memory_hit_num * 100 / total_read))
		print("SSD Hit Ratio : %.2f %%"%( (memory_hit_num + ssd_hit_num) * 100 / total_read))

		#result
		file = 'result/current'
		f =  open(file, 'a+')
		#time,ssd_write,hdd_write,memory_hit,ssd_hit
		f.write("%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\n"%(current_time - 86400,total_write, ssd_write_num , hdd_write_num , (ssd_write_num) * 100 / total_write, hdd_write_num * 100 / total_write, memory_hit_num * 100 / total_read , (memory_hit_num + ssd_hit_num) * 100 / total_read))
		f.close()

if __name__ == '__main__':
	main()	
