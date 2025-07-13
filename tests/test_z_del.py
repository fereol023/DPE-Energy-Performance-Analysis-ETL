# run delete tmp files (see how to handle later)
import shutil
from conftest import *

def test_clean_tmp(test_data_folder):
    shutil.rmtree(os.path.join(test_data_folder, 'tmp'))
