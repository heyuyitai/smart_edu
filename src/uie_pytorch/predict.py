from bs4 import BeautifulSoup
from torch.fx.experimental.accelerator_partitioner import reset_partition_device

from config import config
from data_sync.utils import MySqlReader, Neo4jWriter
from uie_pytorch.uie_predictor import UIEPredictor

class Predictor:
    def __init__(self):

        schema=["TAG"]

        self.course_ie=UIEPredictor(model="uie-base",task_path=str(config.CHECKPOINT_DIR/'course'/'model_best'),schema=schema)
        # self.chapter_ie=UIEPredictor(model="uie-base",task_path=str(config.CHECKPOINT_DIR/'chapter'/'model_best'),schema=schema)
        # self.question_ie = UIEPredictor(model="uie-base", task_path=str(config.CHECKPOINT_DIR / 'question' / 'model_best'), schema=schema)

        self.sql_reader=MySqlReader()
        self.neo4j_writer=Neo4jWriter()



    def _sync_knowledge_point(self,res,label,category,my_ie):
        ans=[]
        for res_dict in res:
            pred=my_ie(res_dict["name"])
            format_pred=[tag["text"] for tag in pred[0]["TAG"]]
            for index,pred in enumerate(format_pred):
                ans.append({
                    "parent_id":res_dict["id"],
                    "name":pred,
                    "id":index
                })
        cypher = f"""
                       UNWIND $batch AS item
                       MERGE (:{label}:{category}{{id:item.id,name:item.name,parent_id:item.parent_id}})
                       """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher,ans)

    def sync_course_knowledge_point(self):
        sql="""
        select id,course_introduce as name
        from course_info
        """
        res=self.sql_reader.read(sql)
        self._sync_knowledge_point(res=res,label="知识点",category="课程",my_ie=self.course_ie)

    def sync_chapter_knowledge_point(self):
        sql = """
              select id,chapter_name as name
              from chapter_info
              """
        res=self.sql_reader.read(sql)
        self._sync_knowledge_point(res, "知识点","章节",my_ie=self.chapter_ie)

    def sync_question_knowledge_point(self):
        sql = """
               select id,question_txt as name
               from test_question_info
               """
        res = self.sql_reader.read(sql)
        for sentence in res:
            sentence['name']=BeautifulSoup(sentence['name'],features="lxml")
            print(sentence)
        self._sync_knowledge_point(res,"知识点","题目",my_ie=self.question_ie)


    def sync_course_to_knowledge(self):
        cypher="""
        MATCH (n:知识点:课程) 
        RETURN n.id as id,n.parent_id as parent_id
        """
        res = self.neo4j_writer.execute_query(cypher)
        ans=[]
        for record in res.records:
            ans.append({
                "end_id":record['id'],
                "start_id":record["parent_id"]
            })
        cypher = f"""
                    UNWIND $batch AS item
                    MATCH (start:Course{{id:item.start_id}}), (end:知识点:课程{{id:item.end_id}})
                    MERGE (start)-[:HAVE]->(end)
                """
        self.neo4j_writer.writeCustomNodeOrRelation(cypher,ans)




if __name__=="__main__":
    predictor=Predictor()
    # predictor.sync_course_knowledge_point()
    # predictor.sync_chapter_knowledge_point()
    # predictor.sync_question_knowledge_point()
    predictor.sync_course_to_knowledge()




