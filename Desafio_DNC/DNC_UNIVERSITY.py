'''
Desafio de competência técnica para a vaga de Analista de Dados Jr. da escola DNC.

Este script consulta o banco de dado University através da interface do MySQL, analisa
sesu dados e plota gráficos para facilitar sua visualização.

'''

from asyncio.windows_events import NULL
import mysql.connector
import matplotlib.pyplot as plt
import numpy as np

def MySQL_Connect(host, database, user, password): #Recebe os atributos para a conexão do banco de dados requerido.
    db = mysql.connector.connect(
    host=host, 
    database=database, 
    user=user,
    password=password)
    return db


def medium_course_rating_plot(mycursor): #Plota a média de rating por curso.
    mycursor.execute("""SELECT 
                     course_id, 
                     CAST(rating AS SIGNED) 
                     FROM course 
                     ORDER BY course_id ASC
                     """) #executa a função SELEECT que extrai as informações da base de dados, o que se repete nas funções de tratamento de dados
    #extrai course_id e rating(função cast transforma rating de varchar para inteiro) do banco course
    #ordena a query por course_id de forma ascendente 
    
    #As funções realizadas pelas linhas de código abaixo serão utilizadas durante todo o código, então só serão comentadas com detalhe aqui.
    table_medium_course_rating_plot = mycursor.fetchall() #Atribui os valores dos dados extraídos para um array.
    course_id = [i[0] for i in table_medium_course_rating_plot] #Atribui os valores de uma coluna do array para um vetor.
    rating = [i[1] for i in table_medium_course_rating_plot]
    plt.bar (course_id, rating, tick_label=course_id) #Plota um gráfico de barras com as arrays selecionadas
    plt.xlabel('ID do curso') #Atribui um tórulo ao eixo x
    plt.ylabel('Rating') #Atribui um rótulo ao eixo y
    plt.title("Média de rating por curso") #Atribui um título ao gráfico
    plt.yticks(np.arange(min(rating), max(rating)+1, 1.0)) #Arruma os ticks do gráfico de acordo com os seus valores mínimos e máximos.
    plt.show() #Mostra o gráfico na interface do matplotlib.


def teacher_popularity_associated_students(mycursor): #Plota a média de popularidade por professor e a quantidade de estudantes associados a esse mesmo professor.
    mycursor.execute("""SELECT 
                        tab_prof.prof_id, 
                        CAST(tab_prof.popularity AS SIGNED), 
                        COALESCE(tab_count.student_count, 0) 
                        FROM prof tab_prof 
                        LEFT JOIN 
                        (SELECT prof_id, COUNT(student_id) AS student_count 
                        FROM ra 
                        GROUP BY prof_id) 
                        tab_count ON tab_prof.prof_id = tab_count.prof_id 
                        ORDER BY tab_prof.prof_id ASC
                        """)
    
    table_teacher_popularity_associated_students = mycursor.fetchall()
    teacher_id = [i[0] for i in table_teacher_popularity_associated_students]
    teacher_popularity = [i[1] for i in table_teacher_popularity_associated_students]
   
    plt.bar (teacher_id, teacher_popularity, tick_label=teacher_id)
    plt.xlabel('ID do Professor')
    plt.ylabel('Popularidade do Professor')
    plt.title("Popularidade dos professores")
    plt.yticks(np.arange(min(teacher_popularity), max(teacher_popularity)+1, 1.0))
    plt.show()
    

def make_table(mycursor): #Cria uma tabela que contém a informação da quantidade de alunos associados a um professor.
    
    mycursor.execute("DROP TABLE IF EXISTS prof_count_aluno")
    mycursor.execute("""CREATE TABLE prof_count_aluno 
                     SELECT prof_id, COUNT(student_id) 
                     AS student_count 
                     FROM ra 
                     GROUP BY prof_id 
                     ORDER BY prof_id""")
    
    
def course_relative_cost(mycursor): #Analisa a relação entre rating do curso e avaliação do salário do professor, apenas para alunos cadastrados em algum curso.
    
    mycursor.execute("""SELECT DISTINCT
                     registro.course_id, 
                     registro.student_id, 
                     CAST(curso.rating AS SIGNED) rating, 
                     COALESCE(CAST(assoc.prof_id AS SIGNED), NULL) prof_id , 
                     COALESCE(assoc.salary, NULL) salary,
                     CASE
						WHEN assoc.salary = 'low' THEN 0
                        WHEN assoc.salary = 'med' THEN 1
                        WHEN assoc.salary = 'high' THEN 2
						END salary_signed
                     FROM registration registro 
                     LEFT JOIN course curso 
                     ON registro.course_id = curso.course_id 
                     LEFT JOIN university.ra assoc 
                     ON registro.student_id = assoc.student_id 
                     ORDER BY registro.course_id ASC
                     """)
    
    table_course_relative_cost = mycursor.fetchall()
    pop_course_id = [i[0] for i in table_course_relative_cost]
    pop_salary_signed = [i[5] for i in table_course_relative_cost]
        
    course_cost = []
    course_index = []

    for i in range (len(pop_course_id)):
        course_cost.append(0)
        
    for i in range (len(pop_course_id)):
        if pop_course_id[i] != None and pop_salary_signed[i] != None:
            course_cost[pop_course_id[i]] = course_cost[pop_course_id[i]] + pop_salary_signed[i]           
        
    for i in range(len(pop_course_id)):
         course_index.append(0)
         
    for i in range(len(pop_course_id)):
        course_index[i] = i
    
    plt.title('Custo relativo de cursos baseado na \n percepção de salário dos professores', fontsize = 12)
    plt.xlabel('ID dos cursos')
    plt.ylabel('Relativo baixo custo                   Relativo alto custo')
    plt.xticks(np.arange(min(course_index), max(course_index)+1, 1.0))
    plt.tick_params(labelleft=False, left=False)
    plt.xlim(3,14)
    
    plt.bar(course_index, course_cost, color ='blue', width = 0.4)
    plt.show()
           

def inteligente_satisfaction(mycursor): #Analisa a relação entre inteligência do aluno e satisfação com o curso.
    mycursor.execute("""SELECT 
                     registro.student_id, 
                     registro.course_id, 
                     CAST(registro.sat AS SIGNED) satisfaction, 
                     CAST(estudante.intelligence AS SIGNED) INTELIGENCE 
                     FROM registration registro 
                     LEFT JOIN student estudante 
                     ON estudante.student_id = registro.student_id 
                     ORDER BY registro.student_id ASC
                     """)
    table_inteligente_satisfaction = mycursor.fetchall()
    
    int_sat_student_id = [i[0] for i in table_inteligente_satisfaction]
    int_sat_satisfaction = [i[2] for i in table_inteligente_satisfaction]
    int_sat_student_intelligence = [i[3] for i in table_inteligente_satisfaction]
    
    fig, ax = plt.subplots()
    ax.bar(np.array(int_sat_student_id)-0.15, int_sat_satisfaction, width = 0.3, color='blue')
    ax2 = ax.twinx()
    ax2.bar(np.array(int_sat_student_id)+0.15, int_sat_student_intelligence, width = 0.3, color='red')
    fig.legend(('Satisfação', 'Inteligência'), loc = "center left")
    ax.set_xlabel('Estudantes classificados por ID', fontsize=13)
    
    plt.title('Comparação da satisfação dos estudantes e sua respectiva inteligência', fontsize = 16)
    plt.xticks(range(min(int_sat_student_id), max(int_sat_student_id)+1))
    plt.tick_params(labelright=False, right=False)
    plt.show()
    
'''__________________________________________________________________________________'''
    
        
if __name__ == '__main__':  #função principal, chama as funções que se conectam ao banco do MySQL e plotam os gráficos.
    db = MySQL_Connect('localhost', 'university', 'root', '30475') #Conecta os atributos do banco de dados selecionado.
    mycursor = db.cursor() #Atribui a uma variável o cursor do banco de dados selecionado.
    medium_course_rating_plot(mycursor) #Plota a média de rating por curso.
    teacher_popularity_associated_students(mycursor) #Plota a média de popularidade por professor e a quantidade de estudantes associados a esse mesmo professor.
    course_relative_cost(mycursor) #Analisa a relação entre rating do curso e avaliação do salário do professor, apenas para alunos cadastrados em algum curso.
    inteligente_satisfaction(mycursor) #Analisa a relação entre inteligência do aluno e satisfação com o curso.
    make_table(mycursor) #Cria uma tabela que contém a informação da quantidade de alunos associados a um professor.
    
    #para ver o próximo gráfico apenas feche o gráfico anterior.