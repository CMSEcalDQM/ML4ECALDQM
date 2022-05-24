import pickle
import numpy as np
import ROOT as r
import pyarrow.parquet as pq

names = ['idx','pt','pho_id','pi0_p4','m','Xtz','ieta','iphi','pho_p4','X','dR']

class ParquetDataset:
    def __init__(self, filename):
        self.parquet = pq.ParquetFile(filename)
        self.cols = None # read all columns
        #self.cols = ['X_jets.list.item.list.item.list.item','y'] 
    def __getitem__(self, index):
        data = self.parquet.read_row_group(index, columns=self.cols).to_pydict()

        data['idx'] = np.int64(data['idx'])
        for name in names:
            if name == 'idx':
                continue
            data[name] = np.double(data[name]) # everything else is doubles

        #data['X_jets'] = np.float32(data['X_jets'][0])  not sure why there's a [0] here
        return dict(data)
    def __len__(self):
        return self.parquet.num_row_groups

class ParquetDatasetLimited:
    def __init__(self, filename):
        self.y = [0] if "Electron" in filename else [1]
        self.parquet = pq.ParquetFile(filename)
        self.cols = ['X','pt','m']  
    def __getitem__(self, index):
        data = self.parquet.read_row_group(index, columns=self.cols).to_pydict()

        data['X'] = np.float32(data['X'][0])
        data['pt'] = np.float32(data['pt'])
        data['m'] = np.int64(self.y)

        data['X'][data['X'] < 1.e-3] = 0. # Zero-Suppression
        #data['X'][-1,...] = 25.*data['X_jets'][-1,...] # For HCAL: to match pixel intensity distn of other layers
        #data['X'] = data['X']/100. # To standardize
        
        return dict(data)
    
    def __len__(self):
        return self.parquet.num_row_groups

class ParquetToNumpy:
    def __init__(self,type,nfiles):
        
        filename = 'data/DoubleElectronPt15To100_pythia8_PU2017_MINIAODSIM.parquet.' if type=='e' else 'data/DoublePhotonPt15To100_pythia8_PU2017_MINIAODSIM.parquet.'
        
        if type != 'e':
            type = 'g'
            
        try:
            self.datadict = pickle.load( open( "obj/"+type+"_"+str(nfiles)+"_dict.pkl", "rb" ) )
            if self.datadict != None:
                return
        except IOError:
            pass

        self.datadict = {}
        for name in names:
            self.datadict[name] = []

        for i in range(nfiles):
            parquet = ParquetDataset(filename+str(i+1))
            for name in names:
                print(i,name)
                for j in range(len(parquet)):
                    self.datadict[name].append(parquet[j][name][0])
        
        for name in names:
            self.datadict[name] = np.stack(self.datadict[name], axis=0)
                    
        pickle.dump( self.datadict, open( "obj/"+type+"_"+str(nfiles)+"_dict.pkl", "wb" ) )

    def __getitem__(self, name):
        return self.datadict[name]

    def getDict(self):
        return self.datadict
        
            
#print 'finaldict'                    
#finaldict = ParquetToNumpy('e',2)
#for name in names:
#    print name,len(finaldict[name])

