echo [$(date)]: "CRETING PYTHON ENVIRNEMNT"
echo [$(date)]: "START"
echo [$(date)]: "creating env with python 3.10 version" 
conda create --prefix ./env python=3.10 -y
echo [$(date)]: "activating the environment" 
source activate ./env
echo [$(date)]: "ENVIRNMENT successfully created"
echo [$(date)]: "INSTALLING requiremnts,txt"
pip install -r requirements.txt
echo [$(date)]: "END"