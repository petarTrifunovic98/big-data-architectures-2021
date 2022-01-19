import subprocess
import os

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]
args = ['hdfs', 'dfs', '-test', '-e', HDFS_NAMENODE + '/batch/seismic_activity_info.csv']
proc = subprocess.Popen("hdfs dfs -test -e " + HDFS_NAMENODE + "/batch/seismic_activity_info.csv", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
#s_output, s_err = proc.communicate()
s_return = proc.returncode
if s_return:
    print(">>>>>>>>>>>>THE FILE DOES NOT EXIST<<<<<<<<<<<<<")
    args = ['hdfs', 'dfs', '-put', '/data/batch/seismic_activity_info.csv', HDFS_NAMENODE + '/batch/seismic_activity_info.csv']
    subprocess.Popen()
    print(">>>>>>>>>>>>FILE IMPORTED<<<<<<<<<<<<<<")
else:
    print(">>>>>>>>>>>>>>THE FILE EXISTS<<<<<<<<<<<<<")