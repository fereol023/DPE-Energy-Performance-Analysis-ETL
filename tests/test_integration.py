import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from conftest import *

def test_connexion_rdms():
    # create sql alchemy engine and ping postgres
    pass

def test_connexion_datalake():
    # ping file storage
    pass

def test_connexion_monitoring():
    # ping elk server
    pass