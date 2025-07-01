import sys
sys.path.insert(0, './')
from test import Test

media_source = 'allegro'
urls = ''


class DeleteAllegro(Test):
    def test_delete(self):
        self.mongo_collection.delete_many({"url": urls})


class TestAllegro(Test):
    filter = {'media_source': media_source, 'url': urls}
    config_doc = list(Test.mongo_collection.find(filter))
    config_doc_base = list(Test.mongo_collection_base.find())

    def test_basic_info(self):
        review_count, review_count_base = 0, 0

        for i in self.config_doc: review_count += 1
        for i in self.config_doc_base: review_count_base += 1

        try:
            self.assertEqual(review_count, review_count_base)
            print("Review count matching")
        except:
            print("Review count is not matching")
            exit()

    def test_body(self):
        try:
            for i, j in zip(self.config_doc, self.config_doc_base): self.assertEqual(i['body'], j['body'])
        except: print("Check the body of review with name {} and review-id {}".format(i['creator_name'], i['id']))