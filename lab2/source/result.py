def printPattern(size):
    print("Generating pattern with size: ", size, '\n')
    for i in range(1, size + 1):
        for j in range(1, i + 1):
            if j % 2 == 0:
                print("* ")
            else:
                print(j, " ")
        print('\n')
    print("Middle section:", '\n')
    for i in range(1, size + 1):
        print("Row ", i, ": ")
        if i % 3 == 0:
            for j in range(1, size + 1):
                if j % 2 == 0:
                    print("E ")
                else:
                    print("O ")
        elif i % 3 == 1:
            for j in range(size, 1 + 1):
                if j > 5:
                    print("B ")
                else:
                    print("S ")
        else:
            for j in range(1, size + 1):
                if j == i:
                    print("X ")
                elif j < i:
                    print("< ")
                else:
                    print("> ")
        print('\n')
    print("Bottom section:", '\n')
    for i in range(size, 1 + 1):
        for j in range(1, size - i + 1):
            print("  ")
        for j in range(1, 2*i - 1 + 1):
            if j == 1  or  j == 2*i - 1  or  i == size:
                print("# ")
            elif j % 3 == 0:
                print(". ")
            else:
                print("+ ")
        print('\n')
    print("Pattern analysis:", '\n')
    totalStars = 0
    totalNumbers = 0
    totalLetters = 0
    for i in range(1, size + 1):
        for j in range(1, i + 1):
            if j % 2 == 0:
                totalStars = totalStars + 1
            else:
                totalNumbers = totalNumbers + 1
    for i in range(1, size + 1):
        if i % 3 == 0:
            totalLetters = totalLetters + size
        elif i % 3 == 1:
            totalLetters = totalLetters + size
        else:
            totalLetters = size
    print("Total stars: ", totalStars, '\n')
    print("Total numbers: ", totalNumbers, '\n')
    print("Total letters: ", totalLetters, '\n')
    print("Total elements: ", totalStars + totalNumbers + totalLetters, '\n')
    print("Pattern generation complete!", '\n')