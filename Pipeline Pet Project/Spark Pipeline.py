from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

'''PySpark Data Frame - SparkSession'''

#if __name__=="__main__":
scSpark = SparkSession.builder.getOrCreate()#... .appName("reading csv").getOrCreate()...

#data_file = '/Users/Malcolm Lewis/Desktop/Pipeline Pet Project/testData*.csv'
#sdfData = scSpark.read.csv(data_file, header=True, sep=",")#.cache()
# print('Total Records = ',sdfData.count())
# # print(sdfData)
# sdfData.show()


'''
SparkSession import from pyspark.sql
converting csv into a df via read.csv
showing contents of df
'''
#------------------------------------------------------------------------------------------------
'''Extraction'''
data_file = '/Users/Malcolm Lewis/Desktop/Pipeline Pet Project/supermarket_sales.csv'
sdfData = scSpark.read.csv(data_file, header=True, sep=",")#.cache()
'''Transformation'''
#---SParkSQL (SQLContext already in SParkSession)---
# gender = sdfData.groupBy('Gender').count()
# gender.show()
sdfData.registerTempTable("sales")
output = scSpark.sql('SELECT Date from sales LIMIT 10')
#output = scSpark.sql('SELECT * from sales WHERE `Unit Price` < 15 AND `Quantity` < 10')
#output = scSpark.sql('SELECT COUNT(*) as total, City from sales GROUP BY City')
output.show()
'''Load'''
#output.write.csv('/Users/Malcolm Lewis/Desktop/Pipeline Pet Project/filtered.json')
output.write.csv("filtered.csv")
# output.write.option("header",True).csv('filtered2')
# output.coalesce(1).write.format('json').save('filtered.json')