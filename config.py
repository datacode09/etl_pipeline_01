# Configuration variables and import statements
import os

src = '/users/prieappcol/BreadReport'
filepath_bread = '/users/prieappcol/Bread/'
filepath_katbat = '/users/prieappcol/Katabat/'
filepath_data = '/users/prieappcol/BreadReport/Data/'
task_name = 'BreadIngestion'
shell_name = 'BreadIngestion.sh'

prefix_dict = {
    'AcctActReturnFile': 'cmc_aar_',
    'AgentReturnFile': 'cmc_atr_',
    'WorkflowReturnFile': 'cmc_wfr_',
    'ProcessedPmtsImportFile': 'pmt_',
    'PmtTransExportFile': 'pmt_',
    'AcctPlacementFile': 'apf_'
}

# Load schemas
with open(f'{src}/Data/bread_schemas.json', 'r') as fp:
    bread_schemas = json.load(fp)
