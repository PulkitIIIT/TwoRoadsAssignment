from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import threading
import time
from random import randint
from multiprocessing import Process, Queue

salesData = {}
totalPartitions = 10

def jsonDataObject():
	d["store_id"] = randint(0,99)
	d["receipt_id"] = randint(0,99)
	d["customer_id"] = randint(0,99)
	itemCount = randint(0,9)
	d["items"] = []
	while(itemCount):
		temp["item_id"] = randint(0,999)
		temp["quantity"] = randint(0,9)
		temp["total_price_paid"] = randint(50,500)
		d["items"].append(temp)
		itemCount = itemCount-1
	return d


class Producer(threading.Thread):
    daemon = True

    def run(self):
        
        producer = KafkaProducer(bootstrap_servers='localhost:9092', 
        	value_serializer=lambda m: json.dumps(m).encode('ascii'))
    	
    	currentSystemTime = int(round(time.time() * 1000))
    	key = str(currentSystemTime)
    	record = jsonDataObject()
    	jsonRecord = json.dumps(record)
    	partitionNo = randint(0,9)
    	producer.send('sales_receipts', partitionNo, key, jsonRecord)

    threading.Timer(1, Producer).start()
		


class Consumer(partitionNo, currentSystemTime, threading.Thread):

    daemon = True
    def run(self):
    	consumer = KafkaConsumer('sales_receipts',
    		group_id = 'my-group',
    		bootstrap_servers=['localhost:9092'],
    		enable_auto_commit=True,
    		value_deserializer=lambda m: json.loads(m.decode('ascii')), 
    		consumer_timeout_ms=1000)
    	consumer.assign([TopicPartition('sales_receipts', partitionNo)])
    	maxTimeWindow = currentSystemTime + 30000
    	localSalesData = {}
    	for message in consumer :
			if int(message.key) <= maxTimeWindow :
				storeId = message.value["store_id"]
				totalSales = 0
				if "items" in message.value :
					for item in message.value["items"]:
						totalSales += item["total_price_paid"]
				if storeId in localSalesData:
					localSalesData[storeId] += totalSales
				else :
					localSalesData[storeId] = totalSales
			else :
				break
		# Updating to salesData so need to be done syncronously
		semaphore = threading.BoundedSemaphore()
		semaphore.acquire()
		for key, value in localSalesData:
			if key in salesData :
				salesData[key] += value
			else :
				salesData[key] = value
		semaphore.release()


class ConsumerUtil(threading.Thread):
	currentSystemTime = int(round(time.time() * 1000))
	for i in range(0,10) :
		Consumer(i, currentSystemTime)
	results = []	
	for key, value in salesData.items():
		temp["store_id"] = key
		temp["total_sales_price"] = value
		singleStoreData = json.dumps(temp)
		results.append(singleStoreData)

	print results
	threading.Timer(30, Consumer).start()


def main():
	threads = [Producer(), ConsumerUtil()]
	for t in threads:
		t.start()

if __name__ == "__main__":
	main()