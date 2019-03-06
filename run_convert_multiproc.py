import os, glob, re
from multiprocessing import Pool

def run_process(process):
    os.system('python %s'%process)

## Parallelize hist -> parquet conversion over multiple runs

# Input path
#xrootd='root://cmsxrootd.fnal.gov' # FNAL
#xrootd='root://eoscms.cern.ch' # CERN
#indir='/eos/uscms/store/user/lpcml/mandrews/IMG'
indir='/afs/cern.ch/work/t/tmudholk/public/NitroDQM'
print(" >> Input file directory: %s"%indir)

# Output path
#outdir='/uscms/physics_grp/lpcml/nobackup/mandrews' # NOTE: Space here is limited, transfer files to EOS after processing
outdir='.'
if not os.path.isdir(outdir):
    os.makedirs(outdir)
print(' >> Output directory: %s'%outdir)

# Runs to parallelize over
runs = [276525]

proc_file = 'convertHistToParquet.py'
processes = ['%s -i %s -o %s -r %s'%(proc_file, indir, outdir, run) for run in runs]
print(' >> Process[0]: %s'%processes[0])

pool = Pool(processes=len(processes))
pool.map(run_process, processes)
