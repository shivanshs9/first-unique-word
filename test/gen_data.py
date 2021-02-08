import random
from nltk.corpus import words
import sys

def writeToFile(dataset, uniqueWord, fileName):
    new_dataset = []
    for word in dataset:
        if word == uniqueWord:
            dupCnt = 1
        else:
            dupCnt = random.randint(2, 4)
        new_dataset.extend([word] * dupCnt)
    random.shuffle(new_dataset)
    print("Word count", len(new_dataset))
    with open(fileName, 'w') as f:
        f.write(' '.join(new_dataset))

dataset = words.words()
unique = sys.argv[1] if len(sys.argv) > 1 else random.choice(dataset)
writeToFile(dataset, unique, "test/data.txt")
