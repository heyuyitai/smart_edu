from datasets import load_dataset

from configuration import config
from data_sync.data_utils import MySqlReader, Neo4jWriter
from uie_pytorch.predict import Predictor


class TextSync:
    def __init__(self):
        self.predictor=Predictor()

    def sync(self):
        self.predictor.sync_course_knowledge_point()
        self.predictor.sync_chapter_knowledge_point()
        # self.predictor.sync_question_knowledge_point()

        self.predictor.sync_course_to_knowledge()
        # self.predictor.sync_chapter_to_knowledge()
        # self.predictor.sync_question_to_knowledge()


if __name__=="__main__":
    text_sync=TextSync()
    text_sync.sync()