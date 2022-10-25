import ir_datasets

data = ir_datasets.load('c4/en-noclean-tr')

for doc in data.docs_iter():
    print(doc)

