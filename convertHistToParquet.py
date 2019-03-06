import glob, re
import ROOT
from root_numpy import hist2array
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

import argparse
parser = argparse.ArgumentParser(description='Convert input root files to parquet format for given runs.')
parser.add_argument('-i', '--indir', default='/afs/cern.ch/work/t/tmudholk/public/NitroDQM', type=str, help='Input dqm root file.')
parser.add_argument('-o', '--outdir', default='.', type=str, help='Output pq file dir.')
parser.add_argument('-r', '--run', default=276525, type=int, help='Run to process.')
args = parser.parse_args()

# Run to process
run = args.run

# List of DQM histogram names to convert
hist_names = [
        'EBOccupancyTask/EBOT rec hit occupancy'
        ,'EBTimingTask/EBTMT timing map'
        ,'EEOccupancyTask/EEOT rec hit occupancy EE -'
        ,'EETimingTask/EETMT timing map EE -'
        ,'EEOccupancyTask/EEOT rec hit occupancy EE +'
        ,'EETimingTask/EETMT timing map EE +'
        ] 

def alphanum_key(s):
    """ Turn string s into a list of string and number chunks.
        "z23a" -> ["z", 23, "a"]
    """
    return [int(c) if c.isdigit() else c for c in re.split('([0-9]+)',s)]

def sort_human(l):
    """ Sort list l into human-readable format 
    """
    l.sort(key=alphanum_key)

def convert_hist2arr(hist_name, f):
    """ Convert ROOT histogram of name hist_name in file f to numpy array.
        Wrapper of root_numpy.hist2array
    """
    run = int(re.search('R000([0-9]+?)__', f).group(1))
    subdet = 'Barrel' if hist_name.startswith('EB') else 'Endcap'
    hist_path = 'DQMData/Run %d/Ecal%s/Run summary/%s'%(run, subdet, hist_name)

    tfile = ROOT.TFile(f)
    hist = tfile.Get(hist_path)

    return hist2array(hist).T

def cleaned_name(hist_name):
    """ Clean up spaces and slashes from hist_name
    """
    return hist_name.replace('/','_').replace(' ','_').replace('+','p').replace('-','m')

####################################################################
# Convert lumi histograms in ROOT DQM file to arrays in parquet file

# Write each run into separate parquet file
print " >> Processing run:", run
dqm_files = glob.glob('%s/DQM_V0001_R000%d__All__Run2016__CentralDAQ_LS*.root'%(args.indir, run))
sort_human(dqm_files)
print " >> N lumi files to process:", len(dqm_files) 
outpath = '%s/ECALDQM_run%d.parquet'%(args.outdir, run)

nLumis = 0
sw = ROOT.TStopwatch()
sw.Start()
# Each lumi file is read and written sequentially to same parquet file
for i,f in enumerate(dqm_files):

    # Get lumi section
    ls = int(re.search('LS([0-9]+?).root', f).group(1))
    if i%100 == 0:
        print "   >> Processing LS %d (%d/%d): %s"%(ls, i+1, len(dqm_files), f)

    # Create dict of TH2 histograms converted to numpy arrays
    # with keys given by cleaned histogram names
    X = {cleaned_name(h):convert_hist2arr(h, f) for h in hist_names}

    # Add supplementary info
    X['run'] = run
    X['ls'] = ls
    #TODO: Add inst. luminosity

    # Convert to parquet-compatible table
    #keys = [cleaned_name(h) for h in hist_names]
    #data = [pa.array([X[k]]) if np.isscalar(X[k]) or type(X[k]) == list else pa.array([X[k].tolist()]) for k in keys]
    data = [pa.array([x]) if np.isscalar(x) or type(x) == list else pa.array([x.tolist()]) for x in X.values()]
    table = pa.Table.from_arrays(data, X.keys())

    # Only need to initialize parquet writer once
    if nLumis == 0:
        writer = pq.ParquetWriter(outpath, table.schema, compression='snappy')

    writer.write_table(table)
    nLumis += 1

writer.close()
sw.Stop()
print " >> nLumis:",nLumis
print " >> Real time:",sw.RealTime()/60.,"minutes"
print " >> CPU time: ",sw.CpuTime() /60.,"minutes"
print " >> Written to:",outpath
print " >> ======================================" 
for k in X.keys():
    if type(X[k]) == np.ndarray: 
        print " >> %s.shape: %s"%(k, str(X[k].shape))
print " >> ======================================" 

# Validate output file
pqIn = pq.ParquetFile(outpath)
print(pqIn.metadata)
print(pqIn.schema)
# Read single row into memory:
X = pqIn.read_row_group(0, ['EBOccupancyTask_EBOT_rec_hit_occupancy.list.item.list.item']).to_pydict()['EBOccupancyTask_EBOT_rec_hit_occupancy'] # list
# Read whole column(s) into memory:
#X = pqIn.read(['EBOccupancyTask_EBOT_rec_hit_occupancy.list.item.list.item']).to_pydict()['EBOccupancyTask_EBOT_rec_hit_occupancy'] # list
X = np.float32(X) # numpy array
print('_________________________________________________________\n')
