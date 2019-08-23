#!/usr/bin/python
# -*- coding: UTF-8 -*-
from sklearn.naive_bayes import GaussianNB
from sklearn.externals import joblib

#http://sklearn.lzjqsdd.com/modules/tree.html
class Classifier():

	def __init__(self):
		self.clf = None
		pass

	def fit(self,X,Y):
		self.clf = self.clf.fit(X, Y)

	def predict(self,X):
		return self.clf.predict(X)

	def save(self,filename):
		joblib.dump(self.clf, "model/%s"%(filename))

	def load(self,filename):
		try:
			self.clf = joblib.load("model/%s"%(filename))
		except BaseException:
			self.clf = GaussianNB()
			return False
		else:
			return True

if __name__ == "__main__":
	filename = "a.clf"
	clf = Classifier()
	if not clf.load(filename):
		X = [[0, 0], [1, 1]]
		Y = [0, 1]
		clf.fit(X,Y)
		print(clf.predict([2,2]))
		clf.save(filename)
	else:
		print("Model Loading Success")
		print(clf.predict([2,2]))
		print(clf.predict([0.2,0.2]))
		print(clf.predict([0.55,0.55]))
		print(clf.predict([0.5,0.5]))
