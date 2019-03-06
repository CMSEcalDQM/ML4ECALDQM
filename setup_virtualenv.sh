# from http://scikit-hep.org/root_numpy/start.html

# Initialize LCG environment
export LCGENV_PATH=/cvmfs/sft.cern.ch/lcg/releases
/cvmfs/sft.cern.ch/lcg/releases/lcgenv/latest/lcgenv -p LCG_89 --ignore Grid x86_64-slc6-gcc62-opt root_numpy > setup_mldqm.sh
echo 'export PATH=$HOME/.local/bin:$PATH' >> setup_mldqm.sh
source setup_mldqm.sh

# Get virtualenv
curl -O https://bootstrap.pypa.io/get-pip.py
python get-pip.py --user
pip install --user virtualenv

# Create virtualenv
ENVNAME=mldqm
virtualenv $ENVNAME 
source $ENVNAME/bin/activate
echo 'source $ENVNAME/bin/activate' >> setup_mldqm.sh
echo '#deactivate' >> setup_mldqm.sh
pip install pyarrow==0.7.1

# For future sessions only need to execute:
#source setup_mldqm.sh
