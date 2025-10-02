import numpy as np
import random
import multiprocessing as mp

def process_file(fileName) -> np.array:
    dict = {'A':[], 'B':[], 'C':[], 'D':[]}
    file = open(fileName, 'r')
    for line in file:
        cat, num = line.split(' ')
        dict[cat].append(float(num))
    file.close()
    return np.array([[np.mean(dict['A']), np.std(dict['A'])],
            [np.mean(dict['B']), np.std(dict['B'])],
            [np.mean(dict['C']), np.std(dict['C'])],
            [np.mean(dict['D']), np.std(dict['D'])]])

files = []
random.seed(0)
for i in range(1, 6):
    fileName = "file" + str(i) + ".csv"
    files.append(fileName)
    file = open(fileName, "w")
    for j in range(500):
        file.write(chr(random.randint(65, 68)) + " " + str(random.random()) + '\n')
    file.close()

if __name__ == "__main__":
    with mp.Pool(processes=5) as pool:
        results = pool.map(process_file, files)
    print(results)
