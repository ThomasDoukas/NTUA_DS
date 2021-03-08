import os
import matplotlib.pyplot as plt
import numpy as np

plt.figure(figsize=(12,8))

barWidth = 0.30
models = ['k=1', 'k=3', 'k=5']
eventual = []
linearizability = []

with open('throughput/insert.out', 'r') as f:
    lines = f.readlines()

for i, line in enumerate(lines):
    tokens = line.split(' ')
    if not i%2:
        eventual.append(float(tokens[7]))
    else:
        linearizability.append(float(tokens[7]))

r1 = np.arange(3)
r2 = [x + barWidth for x in r1]

plt.bar(r1, eventual, color='darkslateblue', width=barWidth, label='Eventual')
plt.bar(r2, linearizability, color='darkred', width=barWidth, label='Linearizability')
plt.xticks([r + barWidth/2 for r in range(3)], models)
plt.title("Time/Ops for consecutive insert operations")
plt.ylabel("Time(s) / operation")
plt.legend(loc=2)
plt.grid(axis="y", linestyle="--")

name = 'write_thr'
if not os.path.exists('plots'):
    os.makedirs('plots')
plt.savefig("plots/" + name + ".png", bbox_inches="tight")