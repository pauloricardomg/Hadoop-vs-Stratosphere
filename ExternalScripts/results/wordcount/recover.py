import os, csv
import glob
import math

nodes=["cloud2","cloud3","cloud4","cloud5","cloud6","cloud7"]

def main():
    MULTIPLIER=[1,2,4,8,16,32]
    for M in MULTIPLIER:
        INPUT_SIZE=str(M*64*6)
        NUM_MAP_TASKS=str(M*6)
        TIME_LOG=open("stratosphere-mr/time_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log")
        log_reader = csv.reader((line.replace(', ', ',').rstrip(',') for line in TIME_LOG))
        row=log_reader.next()
        time=float(row[0])
        for node in nodes:
            BAD_CPU_LOG=open("corrupted/"+node+"/stratosphere-mr/cpu_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log")
            BAD_MEM_LOG=open("corrupted/"+node+"/stratosphere-mr/mem_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log")
            BAD_PROC_LOG=open("corrupted/"+node+"/stratosphere-mr/procs_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log")
            RIGHT_CPU_LOG=open(node+"/stratosphere-mr/cpu_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log", 'w')
            RIGHT_MEM_LOG=open(node+"/stratosphere-mr/mem_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log", 'w')
            RIGHT_PROC_LOG=open(node+"/stratosphere-mr/procs_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log", 'w')
            RIGHT_CPU_LOG.write(', '.join(cut(BAD_MEM_LOG, time)))
            RIGHT_MEM_LOG.write(', '.join(cut(BAD_CPU_LOG, time)))
            RIGHT_PROC_LOG.write(', '.join(cut(BAD_PROC_LOG, time)))
            TIME_LOG.close()
            BAD_CPU_LOG.close()
            BAD_MEM_LOG.close()
            BAD_PROC_LOG.close()
            RIGHT_CPU_LOG.close()
            RIGHT_MEM_LOG.close()
            RIGHT_PROC_LOG.close()
"""
    for M in MULTIPLIER:
        INPUT_SIZE=str(M*64*6)
        NUM_MAP_TASKS=str(M*6)
        for node in nodes:
            CPU_LOG=node+"/hadoop-mr/cpu_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log"
            MEM_LOG=node+"/hadoop-mr/mem_"+INPUT_SIZE+"MB_m"+NUM_MAP_TASKS+"_r6.log"
            os.rename(CPU_LOG,"tmp")
            os.rename(MEM_LOG,CPU_LOG)
            os.rename("tmp",MEM_LOG)
"""
           
            
def cut(log_file, time):
    log_reader = csv.reader((line.replace(', ', ',').rstrip(',') for line in log_file))
    row=log_reader.next()
    ceil = int(math.ceil(time))
    return row[:ceil]

    

if __name__ == "__main__":
            main()