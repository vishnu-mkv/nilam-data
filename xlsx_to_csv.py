from os import walk, system

f = []
for (dirpath, dirnames, filenames) in walk("./xlsx"):
    f.extend(filenames)
    break

for file in f:
    system("in2csv "+ "./xlsx/"+file+ " > " + "./csv/"+file.replace("xlsx", "csv").replace("xls", "csv"))
