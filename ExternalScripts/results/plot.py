"""
Notes:
1.
For now, this module must be placed in the same folder where all the other problem folders are placed
i.e., doing an "ls" in the folder where this module resides will results in:
plot.py
wordcount
kmeans

2.
This module assumes the following structure of each problem folder:
a. each problem folder includes the following folders: hadoop-mr, stratosphere-mr, stratosphere-pact
   and these folders are results from cloud1
b. in addition, each problem folder contains folders: cloud2, cloud3,... cloud7
   and each of these folders contains three folders: hadoop-mr, stratosphere-mr, stratosphere-pact
   which are results from corresponding machines
"""

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.transforms as mtransforms
import numpy as np
import glob
import os
import re
import csv
from matplotlib.patches import Polygon

nodes=["cloud2","cloud3","cloud4","cloud5","cloud6","cloud7"]
plottypes={"cloud2":"ro-","cloud3":"gx-","cloud4":"b^-","cloud5":"ch-","cloud6":"mH-","cloud7":"y+-"}

def main():
    results_folder = raw_input("enter results_folder: ")
    os.chdir(results_folder)
    plot_times_input_sizes("wordcount")
    plot_times_input_sizes("kmeans")
    period=1
    inputSize='1536'
    plot_x_times_for_all_nodes("cpu","wordcount","hadoop-mr",period,inputSize)
    plot_x_times_for_all_nodes("cpu","wordcount","stratosphere-mr",period,inputSize)
    plot_x_times_for_all_nodes("mem","wordcount","hadoop-mr",period,inputSize)
    plot_x_times_for_all_nodes("mem","wordcount","stratosphere-mr",period,inputSize)
    plot_x_times_for_all_nodes("procs","wordcount","hadoop-mr",period,inputSize)
    plot_x_times_for_all_nodes("procs","wordcount","stratosphere-mr",period,inputSize)
    period=2
    inputSize='1536'
    plot_x_times_for_all_nodes("cpu","kmeans","hadoop-mr",period,inputSize)
    plot_x_times_for_all_nodes("cpu","kmeans","stratosphere-mr",period,inputSize)
    plot_x_times_for_all_nodes("mem","kmeans","hadoop-mr",period,inputSize)
    plot_x_times_for_all_nodes("mem","kmeans","stratosphere-mr",period,inputSize)
    plot_x_times_for_all_nodes("proc","kmeans","hadoop-mr",period,inputSize)
    plot_x_times_for_all_nodes("proc","kmeans","stratosphere-mr",period,inputSize)

def plot_times_input_sizes(problem):
    root=os.getcwd()
    os.chdir(problem)
    hadoop_sizes,hadoop_times=get_times_input_sizes("hadoop-mr")
    strat_sizes,strat_times=get_times_input_sizes("stratosphere-mr")
    
    fig = plt.figure()
    ax=fig.add_subplot(111)
    plt.plot(hadoop_sizes, hadoop_times, 'ro-')
    plt.plot(strat_sizes, strat_times, 'bo-')
    ax.set_xlabel('Size of Input (MB)')
    ax.set_ylabel('Time (seconds)')
    plt.figtext(0.70, 0.70, 'Stratosphere', backgroundcolor="blue",
                color='white', weight='roman', size='small')
    plt.figtext(0.70, 0.65, 'Hadoop', backgroundcolor="red",
                color='white', weight='roman', size='small')
    plt.grid(True)
    plt.title('Running Time for '+problem+'\n(Hadoop vs Stratosphere)')
    plt.savefig('time-'+problem)
    os.chdir(root)
    
def get_times_input_sizes(framework):
    problem_dir=os.getcwd()
    os.chdir(framework)
    time_files = glob.glob("time_*")
    time_files = sorted(time_files, key=according_to_input_size)
    sizes=[]
    times=[]
    for log_file in time_files:
	t,s,m,r=parse_log_name(log_file)
	sizes.append(s)
	log_reader = csv.reader((line.replace(', ', ',').rstrip(',') for line in open(log_file, 'r')))
	values=[]
	for row in log_reader:
	    row = filter(lambda x: x!='' , row)
	    values.extend(map(float, row))
	times.append(np.average(values))
    os.chdir(problem_dir)
    return sizes,times

#framework, node, input_size must be string
#period is the sampling period, and is an int
def plot_x_times_for_all_nodes(x,problem,framework,period,inputSize):
    root=os.getcwd()
    os.chdir(problem)
    
    fig = plt.figure()
    ax=fig.add_subplot(111)
    i=0
    ylabel=x
    title=x
    for node in plottypes.keys():
	plottype=plottypes[node]
	xs,times=get_x_time_for_node(x,node,framework,period,inputSize)
	
	#Tune title and label
	if x=="cpu":
	    xs=map(lambda a:float(a)/1200.0,xs)
	    ylabel="CPU"
	    title="CPU utilization"
	elif x=="mem":
	    ylabel="Memory (%)"
	    title="Memory utilization"
	elif x=="procs" or x=="proc":
	    ylabel="Number of Processes"
	    title="Number of processes"
	
	plt.plot(times,xs,plottype)
	
	#Tune y axis limits
	if x=="cpu":
	    plt.ylim((0,1))
	elif x=="mem":
	    plt.ylim((0,20))
	elif x=="procs" or x=="proc":
	    plt.ylim((0,6))
	    
	plt.figtext(0.80, 0.60+0.03*i, node, backgroundcolor=plottype[0],
                color='white', weight='roman', size='small')
	i+=1
    ax.set_xlabel('Time (seconds)')
    ax.set_ylabel(ylabel)
    plt.grid(True)
    plt.title(title+' of all nodes\n('+problem.lower()+', ' + framework.lower()+')')
    plt.savefig(x+'-'+problem.lower()+'-'+framework.lower())
    os.chdir(root)
    
#framework, node, input_size must be string
#period is the sampling period, and is an int
def get_x_time_for_node(x,node,framework,period,input_size):
    problem_dir=os.getcwd()
    os.chdir(node+"/"+framework)
    x_file = glob.glob(x+'_'+input_size+"*").pop()
    xs=[]
    times=[]
    log_reader = csv.reader((line.replace(', ', ',').rstrip(',') for line in open(x_file, 'r')))
    row=log_reader.next()
    row = filter(lambda e: e!='' , row)
    i=1
    for util in row:
	xs.append(util)
	times.append(i*period)
	i+=1
    os.chdir(problem_dir)
    return xs,times


def parse_log_name(log_file):
    delimiters=re.compile('[_\\.]')
    items=delimiters.split(log_file)
    return items[0],items[1].rstrip('MB'),items[2].lstrip('m'),items[3].lstrip('r')

def according_to_input_size(log_file):
    return int(parse_log_name(log_file)[1])
    
if __name__ == "__main__":
            main()
