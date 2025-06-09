import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

os.environ['ENV'] = 'LOCAL'

from utils.fonctions import set_config_as_env_var
from utils.mylogging import log_decorator, logger


@log_decorator
def tast_function():
    return "Hello world"

@log_decorator
def tast_function2():
    logger.info("This is a new log")
    logger.warning("its okey")
    return "Hello world bis"

class aesatClass:
    def run():
        return 0/1

@log_decorator
def aest_function3():
    return TestClass.run()


if __name__=="__main__":
    tast_function()
    tast_function2()
    aest_function3()