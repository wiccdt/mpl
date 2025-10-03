from multiprocessing.pool import ThreadPool
import numpy as np
import random

def process_file(fileName) -> np.array:
    dict = {'A':[], 'B':[], 'C':[], 'D':[]}
    file = open(fileName, 'r')
    for line in file:
        cat, num = line.split(',')
        dict[cat].append(float(num))
    file.close()
    return np.array([[np.median(dict['A']), np.std(dict['A'])],
            [np.median(dict['B']), np.std(dict['B'])],
            [np.median(dict['C']), np.std(dict['C'])],
            [np.median(dict['D']), np.std(dict['D'])]])

files = []
random.seed(0)
for i in range(1, 6):
    fileName = "file" + str(i) + ".csv"
    files.append(fileName)
    file = open(fileName, "w")
    for j in range(500):
        file.write(chr(random.randint(65, 68)) + "," + str(random.random()) + '\n')
    file.close()

if __name__ == "__main__":
    with ThreadPool(processes=5) as pool:
        results = pool.map(process_file, files)
    arr_A = np.vstack((results[0][0], results[1][0], results[2][0], results[3][0], results[4][0]))
    arr_B = np.vstack((results[0][1], results[1][1], results[2][1], results[3][1], results[4][1]))
    arr_C = np.vstack((results[0][2], results[1][2], results[2][2], results[3][2], results[4][2]))
    arr_D = np.vstack((results[0][3], results[1][3], results[2][3], results[3][3], results[4][3]))
    print(arr_A)
    print("А, " + str(np.median(arr_A[:, 0])) + ", " + str(np.std(arr_A[:, 0])))

    print(arr_B)
    print("B, " + str(np.median(arr_B[:, 0])) + ", " + str(np.std(arr_B[:, 0])))

    print(arr_C)
    print("C, " + str(np.median(arr_C[:, 0])) + ", " + str(np.std(arr_C[:, 0])))

    print(arr_D)
    print("D, " + str(np.median(arr_D[:, 0])) + ", " + str(np.std(arr_D[:, 0])))

