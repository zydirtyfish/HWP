#!/usr/bin/python
# -*- coding: UTF-8 -*-

import numpy as np
from LRUCache import LRUCache
from Classifier import Classifier
from datetime import datetime
from sklearn import metrics

import time

feature_num = 14
#cache_size = 327680 # 50 GB/Node
cache_size = 655360 # 100 GB/Node
#cache_size = 1310720 # 200 GB/Node
#memory_size = 26214 # 4 GB/Node
memory_size = 13107 # 2 GB/Node
#memory_size = 6554 # 1 GB/Node

TP = 0
FP = 0
FN = 0
label_y = []
predict_y = []

tablet = {}
normal_set = {}

info_table = {
"ReadSize":0,
"ReadCnt":1,
"BigRead":2,
"SmallRead":3,
"LastRead":4,
"ReadInterval":5,
"WriteSize":6,
"WriteCnt":7,
"BigWrite":8,
"SmallWrite":9,
"LastWrite":10,
"WriteInterval":11,
"Hit":12,
"Predict":13
}

def main():
	global tablet
	global normal_set
	global TP
	global FP
	global FN
	global label_y
	global predict_y

	file_dir = '/cbs_trace1/sample_6/'

	#load Cache
	MemoryCache = LRUCache(memory_size)
	SSDCache = LRUCache(cache_size)

	#load Classifier
	clf = Classifier()

	current_time = 1538323200

	last = 0
	for i in range(1,31):
		ssd_hit_num = 0
		memory_hit_num = 0
		total_read = 0
	
		total_write = 0
		memory_write_num = 0
		memory_write_hit = 0
		ssd_write_num = 0
		hdd_write_num = 0
	
		predict_time = 0
		predict_cnt = 0

		data_x = []
		data_y = []		
	
		if clf.load("%d.clf"%(last)):
			predict_flag = 1
		else:
			predict_flag = 0

		print('----------------------------------')
		cudate = time.strftime("%Y-%m-%d", time.localtime(current_time))
		print(cudate)
		current_time += 86400
		print('----------------------------------')
		file_name = file_dir + str(i)
		f = open(file_name,"r")
		for line in f.readlines():
			io_record = line.split(",")

			#update tablet
			key = io_record[4][:-2]+','+ str(int(io_record[1]) >> 11)
			if key in tablet:
				a = tablet[key]
			else:
				a = [0 for i in range(0,feature_num)]
				tablet[key] = a
			if int(io_record[3]) == 1:
				a[info_table['WriteSize']] += int(io_record[2])
				a[info_table['WriteCnt']] += 1

				if a[info_table['LastWrite']] != 0:
					a[info_table['WriteInterval']] += abs(int(io_record[0]) - a[info_table['LastWrite']])
				a[info_table['LastWrite']] = int(io_record[0])

				if int(io_record[2]) > 64:
					a[info_table['BigWrite']] += 1
				elif int(io_record[2]) <= 8:
					a[info_table['SmallWrite']] += 1
				
			else:
				a[info_table['ReadSize']] += int(io_record[2])
				a[info_table['ReadCnt']] += 1

				if a[info_table['LastRead']] != 0:
					a[info_table['ReadInterval']] += abs(int(io_record[0]) - a[info_table['LastRead']])
				a[info_table['LastRead']] = int(io_record[0])

				if int(io_record[2]) > 64:
					a[info_table['BigRead']] += 1
				elif int(io_record[2]) <= 8:
					a[info_table['SmallRead']] += 1

			#update Cache
			block = io_record[4][:-2]+','+ str(int(io_record[1]) >> 6)
			if int(io_record[3]) == 1:
				#write
				total_write += 1
				if MemoryCache.get(block) != None:
					memory_write_num += 1
					memory_write_hit += 1
				else:
					#reach condition to flush, flush first!
					flush_data = MemoryCache.flush()
					if flush_data != None:
						feature_tmp = []
						flush_key = []
						for key in flush_data:
							flush_key.append(key[0])
							feature_tmp.append(process_eviction(key,0)[0])
						
						if predict_flag == 1:	
							oldtime=datetime.now()
							rst = clf.predict(feature_tmp)
							newtime=datetime.now()
							predict_cnt += len(flush_key)
							predict_time += int((newtime-oldtime).microseconds)
							for key,ft in zip(flush_key,rst):
								tablet_key = "%s,%d"%(key.split(",")[0],int(key.split(",")[1])>>5)
								if key in SSDCache.cache:
									ssd_write_num += 1
									
								else:
									if ft == 1:
										#only write
										hdd_write_num += 1
										tablet[tablet_key][info_table['Predict']] = 1
									else:
										tablet[tablet_key][info_table['Predict']] = 2
										ssd_write_num += 1
										ssd_evict = SSDCache.set(key,1)
										if ssd_evict != None:
											hdd_write_num += 1
											label = process_eviction(ssd_evict,1)
											data_x.append(label[0])
											data_y.append(label[1])
										
						else:
							for key in flush_key:
								ssd_write_num += 1
								ssd_evict = SSDCache.set(key,1)
								if ssd_evict != None:
									hdd_write_num += 1
									label = process_eviction(ssd_evict,1)
									data_x.append(label[0])
									data_y.append(label[1])

					MemoryCache.set(block,1)

			else:
				#read
				total_read += 1
				
				if MemoryCache.get(block) != None:
					#hit
					memory_hit_num += 1
				else:
					if SSDCache.get(block) != None:
						#hit	
						ssd_hit_num += 1
						a[info_table['Hit']] += 1
						
					else:
						ssd_evict = SSDCache.set(block,1)
						ssd_write_num += 1
						if ssd_evict != None:
							hdd_write_num += 1
							label = process_eviction(ssd_evict,1)
							data_x.append(label[0])
							data_y.append(label[1])

		print("SSD footprint : %.2f %%"%(len(SSDCache.cache) * 100 / cache_size))
		print("Memory footprint : %.2f %%"%(len(MemoryCache.cache) * 100 / memory_size))
		print("memory_write : %.2f %%"%(memory_write_num * 100 / total_write))
		print("memory_write_hit : %.2f %%"%(memory_write_hit * 100 / total_write))
		print("ssd_write : %.2f %%"%((ssd_write_num) * 100 / total_write))
		print("hdd_write : %.2f %%"%(hdd_write_num * 100 / total_write))
		print("Memory Hit Ratio : %.2f %%"%(memory_hit_num * 100 / total_read))
		print("SSD Hit Ratio : %.2f %%"%( (memory_hit_num + ssd_hit_num) * 100 / total_read))

		oldtime=datetime.now()
		#training model for next day
		last = i
		#save Classifier
		if len(data_x) > 0:
			clf = Classifier()
			clf.load("%d.clf"%(last))
			clf.fit(data_x,data_y)	
			clf.save("%d.clf"%(last))
		newtime=datetime.now()
		train_time = int((newtime-oldtime).microseconds)

		#result
		file = 'result/hwp_result'
		f =  open(file, 'a+')
	
		#time,memory_write,ssd_write,hdd_write,ssd_write_ratio,hdd_write_ratio,memory_hit,ssd_hit,accuracy,recall,predict_time,auc,train_time,recall2,positive_ratio
		if TP + FP == 0:
			f.write("%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.4f\t%d\t%.2f\t%.2f\n"%(current_time-86400, total_write, ssd_write_num , hdd_write_num , ssd_write_num * 100 / total_write, hdd_write_num * 100 / total_write ,memory_hit_num*100/total_read , (memory_hit_num + ssd_hit_num)*100/total_read , 0 , 0 ,0 , 0, train_time,0,0))
		else:
			auc = metrics.roc_auc_score(label_y,predict_y)
			total = len(label_y)
			right = 0
			pos = 0
			t1 = 0
			for l,p in zip(label_y,predict_y):
				if l == p:
					right += 1
				if l == 1:
					pos += 1
					if p == 1:
						t1 += 1
				
			f.write("%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.4f\t%d\t%.2f\t%.2f\n"%(current_time-86400, total_write, ssd_write_num , hdd_write_num , ssd_write_num * 100 / total_write, hdd_write_num * 100 / total_write , memory_hit_num*100/total_read , (memory_hit_num + ssd_hit_num)*100/total_read , right * 100 / total , TP*100/(TP+FN) , predict_time / predict_cnt , auc , train_time, t1 * 100 / pos , pos*100/total ) )
		f.close()


def process_eviction(ssd_evict,flag):

	global tablet
	global TP
	global FP
	global FN
	global label_y
	global predict_y

	tmp = (ssd_evict[0]).split(",")
	key = "%s,%d"%(tmp[0],int(tmp[1])>>5)
	stat = tablet[key]

	if flag == 1:
		if stat[info_table["Predict"]] != 0:
			if stat[info_table["Hit"]] == 0:
				label_y.append(1)
				if stat[info_table["Predict"]] == 1:
					TP += 1
					predict_y.append(1)
				else:
					FN += 1
					predict_y.append(0)
			else:
				label_y.append(0)
				if stat[info_table["Predict"]] == 1:
					predict_y.append(1)
					FP += 1
				else:
					predict_y.append(0)
		stat[info_table["Predict"]] = 0
	
	result = []
	
	#write ratio
	result.append(int( stat[info_table["WriteCnt"]] * 100 / (stat[info_table["ReadCnt"]] + stat[info_table["WriteCnt"]]) ))
	#------------read-------------
	if stat[info_table["ReadCnt"]] == 0:
		result.append(0)
		result.append(0)
		result.append(0)
		result.append(100)
	else:
		#avg read size
		rz = int(stat[info_table["ReadSize"]]/stat[info_table["ReadCnt"]]) >> 1
		if rz > 100:
			rz = 100
		result.append(rz)
		#big read ratio
		result.append(int(stat[info_table["BigRead"]]*100/stat[info_table["ReadCnt"]]))
		#small read ratio
		result.append(int(stat[info_table["SmallRead"]]*100/stat[info_table["ReadCnt"]]))
		#read interval
		ri = 100
		if stat[info_table["ReadCnt"]] > 1:
			ri = int(stat[info_table["ReadInterval"]]/(stat[info_table["ReadCnt"]] - 1) / 3600)
			if ri > 100:
				ri = 100
		result.append(ri)
	#------------write-------------
	if stat[info_table["WriteCnt"]] == 0:
		result.append(0)
		result.append(0)
		result.append(0)
		result.append(100)
	else:
		#avg write size
		wz = int(stat[info_table["WriteSize"]]/stat[info_table["WriteCnt"]]) >> 1
		if wz > 100:
			wz = 100
		result.append(wz)
		#big read ratio
		result.append(int(stat[info_table["BigWrite"]]*100/stat[info_table["WriteCnt"]]))
		#small read ratio
		result.append(int(stat[info_table["SmallWrite"]]*100/stat[info_table["WriteCnt"]]))
		#read interval
		ri = 100
		if stat[info_table["WriteCnt"]] > 1:
			ri = int(stat[info_table["WriteInterval"]]/(stat[info_table["WriteCnt"]] - 1) / 3600)
			if ri > 100:
				ri = 100
		result.append(ri)

	label = 1
	#label
	if stat[info_table["Hit"]] >= 1:
		label = 0

	return result,label

if __name__ == '__main__':
	main()
