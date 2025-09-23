from bs4 import BeautifulSoup

from data_sync.data_utils import MySqlReader


class Processor:
    def __init__(self):
        self.sql_reader=MySqlReader()

    def beautiful_question_txt(self):
        question_txt=self.sql_reader.getAllQuestionName()
        ans=[]
        for text in question_txt:
            beautiful_txt=BeautifulSoup(text,features='lxml')
            ans.append(beautiful_txt)
        return ans

if __name__=="__main__":
    processor=Processor()
    processor.beautiful_question_txt()

# text  = BeautifulSoup("<p><strong>Person类和Test类的代码如下所示，则代码中的错误语句是</strong></p><p>&nbsp;</p><p>&nbsp;public class Person {</p><p>&nbsp;&nbsp;&nbsp;public String name;</p><p>&nbsp;&nbsp;&nbsp;public Person(String name) {</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;this.name = name;</p><p>&nbsp;&nbsp;&nbsp;}</p><p>}</p><p>public class Test {</p><p>&nbsp;&nbsp;&nbsp;public static void main(String[] args) {</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;final Person person = new Person(“欧欧”);</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;person.name = “美美”;</p><p>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;person = new Person(“亚亚”);</p><p>&nbsp;&nbsp;&nbsp;}</p><p>}</p>",features="lxml")
