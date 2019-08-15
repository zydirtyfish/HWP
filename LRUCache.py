import collections

#基于orderedDict实现
class LRUCache(collections.OrderedDict):

	def __init__(self,size):
		self.size = size
		self.cache = collections.OrderedDict()

	def get(self,key):
		if key in self.cache.keys():
			value = self.cache.pop(key)
			self.cache[key] = value + 1
			return value
		else:
			value = None
			return value

	def set(self,key,value):
		evict = None
		if key in self.cache.keys():
			self.cache.pop(key)
			self.cache[key] = value
		elif self.size == len(self.cache):
			evict = self.cache.popitem(last=False)
			self.cache[key] = value
		else:
			self.cache[key] = value
		return evict

	def flush(self):
		if self.size == len(self.cache):
			result = []
			for i in range(0,int(len(self.cache)/4)):
				result.append(self.cache.popitem(last=False))
			return result
		else:
			return None

if __name__ == '__main__':
	test = LRUCache(5)
	test.set('a',1)
	test.set('f',2)
	test.set('e',3)
	test.set('m',4)
	test.set('d',5)
	print(test.cache)
