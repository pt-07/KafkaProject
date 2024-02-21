import pandas as pd
import matplotlib.pyplot as plt
import json
import os

partition_dir = '/files'

partitions = [0, 1, 2, 3]

months_of_interest = ['January', 'February', 'March']

month_avg_temps = {month: None for month in months_of_interest}
avgs = []
for partition in partitions:
    file_path = os.path.join(partition_dir, f'partition-{partition}.json')
   
    if not os.path.exists(file_path):
        continue

    with open(file_path, 'r') as file:
        data = json.load(file)
    for month in months_of_interest:
        if month in data:
            latest_year = max(data[month].keys(), key=lambda x: int(x))
            #get avg
            avg = data[month][latest_year]["avg"]
            print(avg)
            avgs.append([latest_year, month,avg])
print(avgs)
plot_data = {str(month)+'-' +str(latest_year): float(temp) for latest_year, month,temp in avgs}
fig, ax = plt.subplots()
pd.Series(plot_data).plot.bar(ax=ax)
ax.set_ylabel('Avg. Max Temperature')
ax.set_title('Month Averages')
plt.tight_layout()
plt.savefig("/files/month.svg")
