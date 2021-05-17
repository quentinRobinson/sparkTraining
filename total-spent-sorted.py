from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalSpent")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerId = fields[0]
    spent = float(fields[2])
    return (customerId, spent)

lines = sc.textFile("file:///SparkCourse/customer-orders.csv")
parsedLines = lines.map(parseLine)
toatalByCustomer = parsedLines.reduceByKey(lambda x, y: x + y)
toatalByCustomerSorted = toatalByCustomer.map(lambda x: (x[1], x[0])).sortByKey()
results = toatalByCustomerSorted.collect()

for result in results:
    print(result[1] + "\t{:.2f}".format(result[0]))
