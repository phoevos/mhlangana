import os
import matplotlib.pyplot as plt
import numpy as np

# plt.figure()
barWidth = 0.70

# experiments = [4.3794, 18.6104] # optimized - unoptimized
experiments = [39.15550470352173, 311.4701669216156] # broadcast - repartition
# q = ['Optimized', 'Unoptimized']
q = ['Broadcast Join', 'Repartition Join']

fig = plt.figure()
ax = fig.add_axes([0,0,1,1])
# ax.bar(q, experiments, color='darkslateblue', width=barWidth) # optimized - unoptimized
ax.bar(q, experiments, color='darkolivegreen', width=barWidth) # broadcast - repartition
ax.set_ylabel('Time(s)')
ax.set_title('Query execution times')
ax.grid(axis="y", linestyle="dashdot")

if not os.path.exists('plots'):
    os.makedirs('plots')
# plt.savefig('plots/opt.png', bbox_inches='tight')
plt.savefig('plots/join.png', bbox_inches='tight')
