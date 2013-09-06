import logging, logging.handlers
from connector import util
from connector import bgbase
from connector import bgexport

def configure_logger(path):
	msg_format = "%(asctime)s %(levelname)s \t %(message)s";
	logging.basicConfig(level=logging.DEBUG, format=msg_format)
	handler = logging.handlers.TimedRotatingFileHandler(path, 'D', 1, 30)
	formatter = logging.Formatter(msg_format);
	handler.setFormatter(formatter)
	logging.getLogger('').addHandler(handler);
	return;
	
def run(config):
	exporter = bgexport.SdeToWarehouse(config)
	exporter.run()
	return
		
if __name__ == "__main__":
	config_file = "bgbase.properties"
	config = util.Config(config_file)
	configure_logger(config['exportLogFile'])
	run(config)