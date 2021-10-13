import logging
from datetime import datetime

logging.basicConfig(filename='../log/pipeline_run_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.log',
                    format=datetime.now().strftime("%y/%m/%d %H:%M:%S ") + '%(levelname)s ' + '%(message)s',
                    level=logging.INFO)


logging.info("test success")
