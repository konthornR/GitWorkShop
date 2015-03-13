import tableConfig

dateSetFormat = "%d/%m/%Y"
root_file_path = "C:/Users/admin/Downloads/PSIMS data test/" 
trading_file_path = "/trading/" 
global_configs = [{
					"FileName" : "d_trade.Dat",
					"FilePath" : trading_file_path,
					"DatabaseTableName" : "d_trade",
					"Config" : tableConfig.d_trade_configs
				}]

def getFileConfig(fileName):
	for global_config in global_configs:
		if global_config["FileName"] == fileName:
			return global_config





