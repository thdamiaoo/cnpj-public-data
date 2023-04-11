import sys
import pathlib

path_src = str(pathlib.Path(__file__).parent.resolve()) + '/../'
sys.path.append(path_src)

from leads.leads_apply_filter import Leads

leads = Leads()
leads.export_leads()