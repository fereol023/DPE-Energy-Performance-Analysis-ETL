import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.fonctions import set_config_as_env_var
from utils.mylogging import log_decorator, logger


@log_decorator
def test_function():
    return "Hello world"

@log_decorator
def test_function2():
    logger.info("This is a new log")
    logger.warning("its okey")
    return "Hello world bis"

class TestClass:
    def run():
        return 0/1

@log_decorator
def test_function3():
    return TestClass.run()


if __name__=="__main__":
    test_function()
    test_function2()
    test_function3()