import logging, logging.handlers
from connector import util
from connector import bgbase
from connector import bgimport

def configure_logger(path):
	msg_format = "%(asctime)s %(levelname)s \t %(message)s";
	logging.basicConfig(level=logging.DEBUG, format=msg_format)
	handler = logging.handlers.TimedRotatingFileHandler(path, 'D', 1, 30)
	formatter = logging.Formatter(msg_format);
	handler.setFormatter(formatter)
	logging.getLogger('').addHandler(handler);
	return;
	
def run(config):
	server = config['server']
	database = config['database']
	adminTable = config['adminTable']
	warehouse = bgbase.Warehouse(server, database, adminTable)
	importer = bgimport.WarehouseToSde(warehouse, config)
	importer.run()
	return
		
if __name__ == "__main__":
	config_file = "bgbase.properties"
	config = util.Config(config_file)
	configure_logger(config['importLogFile'])
	run(config)