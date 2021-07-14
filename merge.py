from dbfread import DBF
from pandas import DataFrame
import fastparquet as fastparquet
import os

partitions = list()
for (dirpath, dirnames, filenames) in os.walk('dataset'):
    partitions += [os.path.join(dirpath, file) for file in filenames if 
    dirpath.startswith('dataset/YEAR')
    ]

# partitions = [f'{p[0]}' for p in os.walk('dataset') if p[0].startswith('dataset/YEAR') ] ;
# for file in os.listdir("dataset"):
#     if file.endswith(".pq"):
#         print

# print(partitions)
# fastparquet.writer.merge([f'dataset/UF={uf}_MONTH={month}.pq' for uf in UFs for month in MONTHS])
print(partitions)
fastparquet.writer.merge(partitions)