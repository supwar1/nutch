import numpy as np
from scipy import sparse
from scipy import spatial
import random

a = 'abcdef'
print(a)
print(a[:-1])


'''
def loadSparseMetrix(fileName):
    rows = []
    cols = []
    data = []

    f_read = open(fileName, mode='r')

    inputCount = 0
    maxVal = 0
    for line in f_read.readlines():
        line = line.strip('\n')
        inputList = line.split()

        for i in range(0, int((len(inputList))), 2):
            rows.append(inputCount)
            cols.append(int(inputList[i]) - 1)
            data.append(int(inputList[i + 1]))
            if (int(inputList[i]) > maxVal):
                maxVal = int(inputList[i])

        inputCount += 1

    inputData = sparse.coo_matrix((data, (rows, cols)), shape=(inputCount, maxVal))
    f_read.close()
    return inputData


inputData = loadSparseMetrix('./data/input_v2_x.dat').toarray()
print(inputData)



test = np.array([[1, 2], [3, 4]])
test1 = np.array([[1, 1], [1, 2],[2, 1], [2, 2], [4, 4], [4, 5], [5, 4], [5, 5]])
print('initial array', test1)
numRow = len(test1)
numCol = len(test1[0])
k_value = 3
centers = []
centers.append(test1[3])

# print('center 1', centers[0])

for i in range(k_value - 1):
    distances = [np.linalg.norm(centers[i] - test1[j]) for j in range(numRow)]
    # print('distances', distances)
    dis_sum = sum(distances)
    # distances.sort()
    distances_weight = distances / dis_sum
    # print('distances_weight', distances_weight)
    dice = random.random()
    # print('dice', dice)

    tempSum = 0
    index = 0
    for j in range(len(distances_weight)):
    	tempSum += distances_weight[j]
    	if tempSum >= dice:
    		index = j
    		break;
    # print('index', index)
    centers.append(test1[index])
    # print('center', i + 2, centers[i + 1])

print('centers', centers)
result = {}
valSet = [[] for i in range(k_value)]
for i in range(numRow):
    distanceSet = []
    for j in range(k_value):
        distance = spatial.distance.cosine(test1[i], centers[j])
        
        distance = 0.0
        for k in range(numCol):
            distance += pow(test1[i][k] - centers[j][k], 2)
        distance = np.sqrt(distance)
		
        distanceSet.append(distance)
    index = np.argmin(np.asarray(distanceSet))
    if i == 0:
    	print('current val', test1[i])
    	print('distanceSet', distanceSet)
    	print('index', index)
    # print('*', distanceSet, index)
    # index = distanceSet.index(minVal)
    # print('*', distanceSet)
    # print(index, i, j, k)
    valSet[index].append(test1[i])
    result[i] = index + 1

print(valSet)
awl = spatial.distance.cosine([1,2], [2,2])
print(awl)
'''










