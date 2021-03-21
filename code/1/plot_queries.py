import os
import matplotlib.pyplot as plt
import numpy as np

# find ./ -name '*.out' | xargs -I files cp files output

plt.figure(figsize=(12,8))
barWidth = 0.30

files = os.listdir('../../output/1')
experiments = []
csv, parquet, rdd = [], [], []
q = ['Q1', 'Q2', 'Q3', 'Q4', 'Q5']

for file in files:
    name = file.split('.')[0]
    with open('../../output/1/' + file, 'r', encoding="utf8") as f:
        lines = f.readlines()
    time = lines[-2].split()[1]
    experiments.append((name, time))

queries = np.array_split(experiments, 5)
for i in range(5):
    csv.append(float(queries[i][0][1]))
    parquet.append(float(queries[i][1][1]))
    rdd.append(float(queries[i][2][1]))

r1 = np.arange(5)
r2 = [x + barWidth for x in r1]
r3 = [x + barWidth for x in r2]

plt.bar(r1, csv, color='darkslateblue', width=barWidth, label='CSV')
plt.bar(r2, parquet, color='darkred', width=barWidth, label='Parquet')
plt.bar(r3, rdd, color='olivedrab', width=barWidth, label='RDD')
plt.xticks([r + barWidth for r in range(5)], q)
plt.title("Query execution times")
plt.ylabel("Time(s)")
plt.legend(loc=2)
plt.grid(axis="y", linestyle="--")

if not os.path.exists('plots'):
    os.makedirs('plots')
plt.savefig("plots/queries.png", bbox_inches="tight")
