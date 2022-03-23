# ADF_MovieLens


________________________________________________________________________________________________________________________________________________________________

Neste repo vamos abordar a Arquitetura para um SQL Server baseado nos dados da MovieLens, desde sua normalização até a criação de procedures e views.  

#Link MovieLens
_____________________________________________________________________________________________________________________________________________________________


 ABORDAGEM
___________________________________________________________________________

Neste repo vamos abordar a Arquitetura para um SQL Server baseado nos dados da MovieLens.  
Criar modelo de dados que comporte:
  Filmes (Movies)
  Gêneros (Genres)
  Locações (Rent)
  Usuários (Users)
  Cópias para locação (Copy)
  Avaliações (Ratings)
  
Você deverá implementar as seguintes funcionalidades em SQL
  Alugar um filme
  Devolver um filme
  Verificar filmes com retorno em atraso
  Verificar filmes que foram entregues com atraso
  Dado um filme, verificar quais cópias físicas estão disponíveis para locação
  Criar cópia de um filme no banco

 
TABELAS
___________________________________________________________________________

A normalização das tabelas se dá da seguinte forma:
A tabela filmes vai receber os dados do CSV movies.
A tabela gênero vai precisar receber os gêneros dos filmes do CSV movies.
Vamos precisar de uma tabela para linkar os gêneros com os respectivos filmes por questões de normalização.
A tabela Ratings vai receber os dados do CSV ratings.
A tabela Copy vai receber as cópias dos filmes basedo no CSV tags
A tabela Users vai receber os dados dos usuários baseados do CSV users.
Vamos precisar de uma tabela com os respectivos dados dos filmes alugados, o dia em que foi alugado, sua expectativa de devolução e a real devolução, por questões também de normalização.

Diagrama das tabelas:

![image](https://user-images.githubusercontent.com/81394440/159702446-ec4cea78-aa48-4331-9a3c-032d0f646ead.png)

AZURE DATA FACTORY
___________________________________________________________________________

O objetivo deste caso de uso é utilizar as ferramentas da Azure para resolução e desenvolvimento, criando um SQL Server, fazer o ETL e Sink dos dados através do Azure Data Factory.
Provisionar:
Resource  Group.
Azure SQL Server
Azure Data Factory
Storage Account (Formato DataLake)
Azure Key Vault (Opcional para guardar as Chaves)
No ADF devemos configura-lo com os linked services que formos usar, por exemplo para um SQL Server devemos configurar um Linked Service que vai integrar o Data Factory com o SQL Server, sendo assim podemos tanto ler os arquivos que estão no servidor como também persistir os dados através das pipelines.

ETL/Sink do SQL Server:
É muito importante avaliar os dados para saber como podemos importar eles para no SQL Server de maneira que a normalização faça sentido.
Observamos que no movies.csv os dados estão da seguinte forma:

![image](https://user-images.githubusercontent.com/81394440/159702520-4ddeeb76-6dd2-4f89-be42-930d67bf259e.png)

Sendo assim vamos usar um pipeline de cópia utilizando os dados do movieid e title respectivamente para a tabela Movies em nosso banco, assim como da tabela Ratings vamos pegar somente as colunas necessárias e do Users também (analisar dados CSV).


Com nosso banco criado e nosso SQL Server devemos copiar os dados com um pipeline:

![image](https://user-images.githubusercontent.com/81394440/159702574-1a003503-7f3c-45d9-a31a-63a0fc701145.png)

Este Pipeline vai copiar os dados para as respectivas tabelas.
Movies, Ratings e Users.

![image](https://user-images.githubusercontent.com/81394440/159702610-c1c6bd06-117d-4a3d-8504-bd810ad7fcfc.png)

Utilizando o DataFlow:
Como observado no nosso csv os gêneros estão todos no csv de movies e não temos eles de forma distinta, além deles estarem concatenados com uma string “|”.

![image](https://user-images.githubusercontent.com/81394440/159702669-3ea7b5c5-cd8e-440a-a262-9060dfa893c3.png)

Sendo assim vamos usar o Data Flow para dar um split, baseado nessa string “|” e depois agrupar e contar quantos filmes de cada gênero temos, assim teremos eles de forma distinta além de uma contagem que pode ser útil no futuro.
Filtrando Generos Distintos:

![image](https://user-images.githubusercontent.com/81394440/159702716-ee641da4-3a4b-4316-ae8a-284070933a3b.png)
![image](https://user-images.githubusercontent.com/81394440/159702743-e6411b72-2c0c-4ca4-9ee9-2e355024440f.png)
![image](https://user-images.githubusercontent.com/81394440/159702765-dc17d418-0397-4495-bb2a-b062088a1f3b.png)
![image](https://user-images.githubusercontent.com/81394440/159702782-36550c28-32ab-4920-96c1-8cb09c1def1a.png)

Uma vez com os gêneros distintos filtrados vamos criar um pipeline para debugar o Data Flow e inserir os dados no banco. A partir desta tabela vamos ter o index de cada gênero, sendo assim teremos como passar os dados para a tabela de Genres_Link e assim ter uma normalização adequada.
Debugando DataFlow:

![image](https://user-images.githubusercontent.com/81394440/159702807-a1b9d133-5a85-4e49-81dc-cd6b394516b5.png)

Genres_Linked ETL.

![image](https://user-images.githubusercontent.com/81394440/159702837-ee0f476a-7107-484e-81f2-21a3f27e6aa0.png)

Linkando Generos:

![image](https://user-images.githubusercontent.com/81394440/159702871-84a2e12f-efb8-494a-a880-ee9f27b6bb68.png)

Vamos utilizar o Split da mesma maneira que no passo acima e também o Flatern, porém com base nos dados vindos de nossa tabela com os respectivos ids dos gêneros distintos vamos fazer um join para que os dados sejam inseridos no nosso banco.

![image](https://user-images.githubusercontent.com/81394440/159702928-2301b7ac-beae-46da-967b-078e47b2038c.png)

![image](https://user-images.githubusercontent.com/81394440/159702939-024999e7-0150-4694-ad9c-6e9a5ce73f12.png)

Com a query podemos ver que os dados estão linkados corretamente entre gêneros e filmes:
SELECT MOVIE_ID, TITLE, GENRES, GENRE_ID 
FROM Genres_Link AS G
JOIN Movies AS M
ON M.DATASET_ID = G.MOVIE_ID
JOIN Genres AS GE
ON GE.ID = G.GENRE_ID
ORDER BY G.MOVIE_ID;

![image](https://user-images.githubusercontent.com/81394440/159703061-ad7a9185-2089-4064-8fff-5a02ba800ffe.png)

Transformando CSV de tags em Copy:
Em termos de aprendizado e também para termos os dados em nossa tabela vamos fazer uma transformação no tags.csv.

![image](https://user-images.githubusercontent.com/81394440/159703127-8a8ea713-e6d8-40cf-bef8-7529fd4756a8.png)

Nós vamos utilizar o movieId como a cópia por exemplo o movieId 60756 tem três dados na nossa imagem ou seja serão três cópias respectivamente deste filme, e a coluna timestamp será quando o cópia foi criada.

![image](https://user-images.githubusercontent.com/81394440/159703167-0a4c31ca-3bfd-4f3b-93e5-a04f37a435f5.png)

toTimestamp(toInteger(toString(byName('timestamp')))*1000l,'YYYY-MM-DD hh:mm:ss') 
Cast coluna timestamp para data.

Resumidamente o andamento do Data Flow para filtrar e dar sink através das pipelines são estes.
Agora vamos demonstrar como executar as Store Procedures através dos pipelines.
Lembrando que as Procedures já tem que estar criadas no nosso SQL Server.
Todos os Scripts de criação das Tabelas, Querys, Procedures e Views Estão no repositório Git.

![image](https://user-images.githubusercontent.com/81394440/159705245-3b4556dc-723d-4131-998b-ad44506c32e1.png)

Parâmetros para serem passados para as Procedures:

![image](https://user-images.githubusercontent.com/81394440/159705293-d021631e-11eb-4030-a8b2-be318204af66.png)
![image](https://user-images.githubusercontent.com/81394440/159705316-a01ef684-6821-4494-a19d-d358bbe8e544.png)

Assim chegamos ao fim, podemos com o Dataflow fazer uma carga de dados e rodar eles como uma "esteira" de ingestão de dados, supondo que esses dados cheguem de um link de atualização por exemplo poderiamos criar triggers para sempre fazer a ingestão destes dados atualizados!!!

___________________________________________________________________________
https://grouplens.org/datasets/movielens/
