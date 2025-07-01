import json
import requests
from datetime import date, datetime
from confluent_kafka import Producer
from scrapy.conf import settings
import os, configparser


class kafkaProducer:

    def __init__(self):
        config = configparser.ConfigParser()
        config.read(os.path.expanduser('~/.setuserv/kafka.ini'))
        kafka_config = config['Kafka']
        kafka_server = kafka_config['KAFKA_SERVER']
        self.producer = Producer({'bootstrap.servers': kafka_server})

    def default(self, o):
        from datetime import date, datetime
        if isinstance(o, (date, datetime)):
            return o.isoformat()

    def delivery_report(self, err, msg):
        print('In delivery_report method')
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} {} [{}]'.format(msg.topic(), msg.key(),
                                                           msg.partition()))

    def setu_producer(self, doc, topic_name):
        print('doc ==>', doc)
        # message_key = doc['client_id'].encode('utf-8')

        if doc:
            print('topic_name in queue_scraped_review_data-->', topic_name)
            try:
                print("producer type in setu_spider - ", type(self.producer))
                print("producer in setu_spider - ", self.producer)
                if 'client_id' in doc:
                    print('message with client id')
                    message_key = doc['client_id'].encode('utf-8')
                    self.producer.produce(topic_name,
                                          json.dumps(doc, default=self.default).encode('utf-8'),
                                          key=message_key,
                                          callback=self.delivery_report)
                else:
                    print('message with no client id')
                    self.producer.produce(topic_name,
                                          json.dumps(doc, default=self.default).encode('utf-8'),
                                          callback=self.delivery_report)
                self.producer.flush()
            except Exception as exc:
                print(exc)
                print("Exception while producing from setu_spider")

        print("Exiting the queue_scraped_review_data method in setuserv_spider")


class MFILogs:
    def __init__(self):
        pass

    def scraper_logs(self, scraper_type, original_url, start_date, page_no, gsheet_id, status,client_id):

        print("untils_logs ##########################################################")
        print("gsheet_id", gsheet_id)

        message = {'Scraper Type': scraper_type, 'original_url': original_url,
                   'Input_Start_Date(YYYY/MM/DD)': start_date, 'page_no': page_no, 'status': status}
        topic_name = str(client_id) + str(gsheet_id)
        print('message_request', message)
        SetuProducers = kafkaProducer()
        if len(gsheet_id) != 0:
            print('Gsheet_id is present')
            SetuProducers.setu_producer(message, topic_name)
        else:
            print('Logs are not sending to gsheet as there is no ghseet id is pesent')

class payment_gateway_api:

    config_scraping = configparser.ConfigParser()
    config_scraping.read(os.path.expanduser('~/.setuserv/kafka.ini'))
    payments_config = config_scraping['Kafka']
    def __init__(self):
        self.PAYMENT_URL = str(payment_gateway_api.payments_config['PAYMENT_URL'])

    def get_email_id(self, client_id):
        URL = self.PAYMENT_URL+'/read_client_email_data/'
        try:
            print(client_id)
            payload = {
                      "filters": str({"client_id":f'{client_id}'})
                      };
            header = {"Content-type": "application/json","Accept":"text/plain"}
            # sending get request and saving the response as response object
            res = requests.post(url = URL, data = json.dumps(payload), headers=header)
            print("RES:",res)
            response_dict = json.loads(res.text)
            res_status = json.loads(json.dumps(response_dict['status']))
            res_replace = res_status.replace("\'", "\"")
            res_replace = json.loads(res_replace)
            print(res_replace[0]['email_id'],type(res_replace))
            return res_replace[0]['email_id']
        except Exception as msg:
            raise msg

    def get_quota(self, email_id):

        URL = self.PAYMENT_URL+'/get_quota/'
        # defining a payload dict for the parameters to be sent to the API
        try:
            print(email_id)
            payload = {
                      "email_id": str({"email_id":f'{email_id}'})
                      };
            header = {"Content-type": "application/json","Accept":"text/plain"}
            print("Payload:",payload)
            # sending get request and saving the response as response object
            res = requests.post(url = URL, data = json.dumps(payload), headers=header)
            print("Quota Left:",res.json()['Quota'])
            return res.json()['Quota']
        except Exception as msg:
            raise msg

    def update_quota(self, email_id):

        URL = self.PAYMENT_URL+'/update_quota/'
        # defining a payload dict for the parameters to be sent to the API
        try:
            print(email_id)
            payload = {
                      "email_id": str({"email_id":f'{email_id}'})
                      };
            header = {"Content-type": "application/json","Accept":"text/plain"}
            print("Payload:",payload)
            # sending get request and saving the response as response object
            res = requests.post(url = URL, data = json.dumps(payload), headers=header)
            print("Updated Quota:",res.json()['Quota'])
            return res.json()['Quota']
        except Exception as msg:
            raise msg
