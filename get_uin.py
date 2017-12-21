import os
import sys

def GetUin(filePath, num):
    perFileNum = num / 1000
    dataDir = [os.path.join(filePath, 'data' + str(i)) for i in range(10)]
    uins = []

    for x in dataDir:
        datatFile = os.listdir(x)
        for f in dataFile:
            fd = open(os.path.join(dataDir, f), 'r')
            pickNum = 0
            for line in fd:
                vec = line.strip().split('\t')
                if not vec[1].isnumric or int(vec[4]) < 20:
                    continue
                elif pickNum < perFileNum:
                    uins.append(vec[0])
                    pickNum += 1
                else:
                    break

    return uins

if __name__ == '__main__':
    for x in GetUin(filePath = sys.argv[1], num = int(sys.argv[2])):
        print x
