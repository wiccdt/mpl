#include <iostream>
using namespace std;

void analyzeNumbers(int numbers[], int size) {
    cout << "Starting number analysis..." << endl;
    cout << "Array size: " << size << endl;

    int positiveCount = 0;
    int negativeCount = 0;
    int zeroCount = 0;
    int evenCount = 0;
    int oddCount = 0;

    for(int i = 0; i < size; i++) {
        cout << "Processing number: " << numbers[i] << endl;

        if(numbers[i] > 0) {
            positiveCount++;
            cout << "Found positive number" << endl;
        } else if(numbers[i] < 0) {
            negativeCount++;
            cout << "Found negative number" << endl;
        } else {
            zeroCount++;
            cout << "Found zero" << endl;
        }

        if(numbers[i] % 2 == 0) {
            evenCount++;
            cout << "Number is even" << endl;
        } else {
            oddCount++;
            cout << "Number is odd" << endl;
        }
    }

    cout << "=== STATISTICS ===" << endl;
    cout << "Positive numbers: " << positiveCount << endl;
    cout << "Negative numbers: " << negativeCount << endl;
    cout << "Zeros: " << zeroCount << endl;
    cout << "Even numbers: " << evenCount << endl;
    cout << "Odd numbers: " << oddCount << endl;

    int maxNumber = numbers[0];
    int minNumber = numbers[0];

    for(int i = 1; i < size; i++) {
        if(numbers[i] > maxNumber) {
            maxNumber = numbers[i];
            cout << "New maximum found: " << maxNumber << endl;
        }

        if(numbers[i] < minNumber) {
            minNumber = numbers[i];
            cout << "New minimum found: " << minNumber << endl;
        }
    }

    cout << "Maximum number: " << maxNumber << endl;
    cout << "Minimum number: " << minNumber << endl;

    int range1 = 0;
    int range2 = 0;
    int range3 = 0;
    int range4 = 0;
    int range5 = 0;

    for(int i = 0; i < size; i++) {
        if(numbers[i] >= -100 && numbers[i] <= -10) {
            range1++;
            cout << "Number in range -100 to -10" << endl;
        } else if(numbers[i] >= -9 && numbers[i] <= -1) {
            range2++;
            cout << "Number in range -9 to -1" << endl;
        } else if(numbers[i] == 0) {
            range3++;
            cout << "Number is zero" << endl;
        } else if(numbers[i] >= 1 && numbers[i] <= 9) {
            range4++;
            cout << "Number in range 1 to 9" << endl;
        } else if(numbers[i] >= 10 && numbers[i] <= 100) {
            range5++;
            cout << "Number in range 10 to 100" << endl;
        }
    }

    cout << "=== RANGE ANALYSIS ===" << endl;
    cout << "Range -100 to -10: " << range1 << endl;
    cout << "Range -9 to -1: " << range2 << endl;
    cout << "Zeros: " << range3 << endl;
    cout << "Range 1 to 9: " << range4 << endl;
    cout << "Range 10 to 100: " << range5 << endl;

    cout << "Analysis complete!" << endl;
}