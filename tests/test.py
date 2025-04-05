import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.scripts import make_logs_backup
from utils.fonctions import get_today_date, get_yesterday_date

make_logs_backup()