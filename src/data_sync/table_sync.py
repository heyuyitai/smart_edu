from config.config import NEO4J_CONFIG
from data_sync.utils import MySqlReader, Neo4jWriter


class TableSync:
    def __init__(self):
        self.sql_reader=MySqlReader()
        self.neo4j_writer=Neo4jWriter()

    def sync_category(self):
        sql="""
        select id,
        category_name as name
        from base_category_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Category",res)

    def sync_subject(self):
        sql="""
        select id,
        subject_name as name
        from base_subject_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Subject",res)

    def sync_course(self):
        sql="""
        select id,
        course_name as name
        from course_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Course",res)

    def sync_teacher(self):
        sql="""
        SELECT 
        DISTINCT teacher AS name 
        FROM course_info;
        """
        res=self.sql_reader.read(sql)
        for index,dict_info in enumerate(res):
            dict_info["id"]=index
        self.neo4j_writer.writeNode("Teacher",res)

    def sync_price(self):
        sql = """
           SELECT 
           DISTINCT CAST(origin_price AS FLOAT) AS name
           FROM course_info;
           """
        res = self.sql_reader.read(sql)
        for index, dict_info in enumerate(res):
            dict_info["id"] = index
        self.neo4j_writer.writeNode("Price", res)

    def sync_chapter(self):
        sql="""
        select id,
        chapter_name as name
        from chapter_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Chapter",res)

    def sync_video(self):
        sql="""
        select id,
        video_name as name
        from video_info
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Video",res)

    def sync_paper(self):
        sql="""
        select id,
        paper_title as name
        from test_paper
        """
        res=self.sql_reader.read(sql)

        self.neo4j_writer.writeNode("Paper",res)

    def sync_question(self):
        sql="""
        select id,
        question_txt as name
        from test_question_info
        """
        res=self.sql_reader.read(sql)
        self.neo4j_writer.writeNode("Question",res)




if __name__=="__main__":
    table_sync=TableSync()
    table_sync.sync_category()
    table_sync.sync_subject()
    table_sync.sync_course()
    table_sync.sync_teacher()
    table_sync.sync_price()
    table_sync.sync_chapter()
    table_sync.sync_video()
    table_sync.sync_paper()
    table_sync.sync_question()