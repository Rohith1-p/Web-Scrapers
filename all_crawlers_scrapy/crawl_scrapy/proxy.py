import time
def fib(x):
    x = int(x)
    val = 5
    prev_val = 3
    for i in range(x):
        temp = val
        val = prev_val + val
        prev_val = temp
    return val

class baseproxymiddleware(object):

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def __init__(self, settings):
        self.retry_counter = dict()

    def gen_key_prodid_url_page(self, request):
        prod_id = request.meta['media_entity']['id']
        print("PRODID: ",prod_id)
        url = str(request.url)
        if 'page' in request.meta:
            page = request.meta['page']
        elif 'page_count' in request.meta:
            page = request.meta['page_count']
        elif 'offset' in request.meta:
            page = request.meta['offset']
        else:
            page = 'no_page_in_meta'
        key = (prod_id,url,page)
        return key, prod_id, url, page

    def update_key(self, request , key):
        if key not in self.retry_counter:
            self.retry_counter[key] = 1
            print(self.retry_counter[key])
        else:
            self.retry_counter[key] += 1
            print(self.retry_counter[key])

    def dump(self, response, ext, *params):
        try:
            dump_time = str(time.time()).replace(".","")
            filename = '/tmp/' + "_".join(params) + '_' + dump_time
            f = open(filename + "." + ext, "w+")
            f.write(response.text)
            f.close()
            print("name of the dump file is: ", filename)
        except:
            print("dumping failed")
            print("exception: ", traceback.print_exc())
            self.logger.info("dumping html file failed")
