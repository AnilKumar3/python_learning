#
#import sys
import subprocess
#function to call shell command
def run_cmd(args_list):
        #run linux commands
        print('Running system command: {0}'.format(' '.join(args_list)))
        proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        s_output, s_err = proc.communicate()
        s_return =  proc.returncode
        return s_return, s_output, s_err

#Run Hadoop ls command in Python to get all the files
(ret, out, err)= run_cmd(['hdfs', 'dfs', '-ls', '/raw/LOGGING/hdp_application_xxxx/2018/10/07/'])
for lines in out.split('\n'):
        file1 = '/'+lines.split(" /")[-1]
        print file1
        print ""
        print ""
        #To get content of the file
        (ret1,out1,err1)=run_cmd(['hdfs', 'dfs', '-text',file1])
        for jlines in out1.split('\n'):
                jfile1=jlines.split("\t")[-1]
                print jfile1


