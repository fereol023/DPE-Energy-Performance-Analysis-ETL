import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.scripts import make_logs_backup
from src.pipelines import S3_BUCKET_NAME

if __name__=="__main__":
    
    # migrate logs daily to s3 bucket and delete previous day logs 
    make_logs_backup(S3_BUCKET_NAME)