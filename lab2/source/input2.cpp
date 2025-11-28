#include <iostream>
using namespace std;

void printPattern(int size) {
    cout << "Generating pattern with size: " << size << endl;

    for(int i = 1; i <= size; i++) {
        for(int j = 1; j <= i; j++) {
            if(j % 2 == 0) {
                cout << "* ";
            } else {
                cout << j << " ";
            }
        }
        cout << endl;
    }

    cout << "Middle section:" << endl;

    for(int i = 1; i <= size; i++) {
        cout << "Row " << i << ": ";

        if(i % 3 == 0) {
            for(int j = 1; j <= size; j++) {
                if(j % 2 == 0) {
                    cout << "E ";
                } else {
                    cout << "O ";
                }
            }
        } else if(i % 3 == 1) {
            for(int j = size; j >= 1; j--) {
                if(j > 5) {
                    cout << "B ";
                } else {
                    cout << "S ";
                }
            }
        } else {
            for(int j = 1; j <= size; j++) {
                if(j == i) {
                    cout << "X ";
                } else if(j < i) {
                    cout << "< ";
                } else {
                    cout << "> ";
                }
            }
        }
        cout << endl;
    }

    cout << "Bottom section:" << endl;

    for(int i = size; i >= 1; i--) {
        for(int j = 1; j <= size - i; j++) {
            cout << "  ";
        }

        for(int j = 1; j <= 2*i - 1; j++) {
            if(j == 1 || j == 2*i - 1 || i == size) {
                cout << "# ";
            } else if(j % 3 == 0) {
                cout << ". ";
            } else {
                cout << "+ ";
            }
        }
        cout << endl;
    }

    cout << "Pattern analysis:" << endl;
    int totalStars = 0;
    int totalNumbers = 0;
    int totalLetters = 0;

    for(int i = 1; i <= size; i++) {
        for(int j = 1; j <= i; j++) {
            if(j % 2 == 0) {
                totalStars = totalStars + 1;
            } else {
                totalNumbers = totalNumbers + 1;
            }
        }
    }

    for(int i = 1; i <= size; i++) {
        if(i % 3 == 0) {
            totalLetters = totalLetters + size;
        } else if(i % 3 == 1) {
            totalLetters = totalLetters + size;
        } else {
            totalLetters = size;
        }
    }

    cout << "Total stars: " << totalStars << endl;
    cout << "Total numbers: " << totalNumbers << endl;
    cout << "Total letters: " << totalLetters << endl;
    cout << "Total elements: " << totalStars + totalNumbers + totalLetters << endl;

    cout << "Pattern generation complete!" << endl;
}