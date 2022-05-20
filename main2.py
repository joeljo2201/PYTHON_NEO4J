from neo4j import GraphDatabase
import numpy as np
import random
import pandas as pd
customer_id = [i for i in range(1,501)]
paymentoptions = ['EMI', 'OnDelivery','OnCredit','OnDebit','Vochers','Giveaways']
randints = [random.randint(0,5) for i in range(0,500)]
payops = [paymentoptions[i] for i in randints]
Cost = np.random.randint(low = 20, high = 12500, size = (500))
Item_types = ['Mobile', 'TV','LED lights','Computer','headphones','mouse']

random_ints = [random.randint(0,5) for i in range(0,500)]
Items = [Item_types[i] for i in random_ints]

records = {"customer_id": customer_id,
                   "Payment_Option" : payops,
                   "Cost" : list(Cost),
                    "Item" : Items,}

df = pd.DataFrame(records)
# print(df)
querylist = df.values.tolist()
itterablelist=[]
# print(querylist)
for i in  querylist:
    query="CREATE (node:Records{customer_id:" + str(i[0]) +", Payment_Option: '" + str(i[1]) +"', Cost: " + str(i[2]) +", Item: '" + str(i[3]) + "'})"
    itterablelist.append(query)
# print(itterablelist)


def Store_in_db(param):
    dbcon = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j","root"))
    sess = dbcon.session()
    for q in param:
        sess.run(q)

# Store_in_db(itterablelist)

def relations():
    dbcon = GraphDatabase.driver(uri="bolt://localhost:7687", auth=("neo4j", "root"))
    sess = dbcon.session()
    query="CREATE (node:lights{})"
    sess.run(query)
    query = "CREATE (node:others{})"
    sess.run(query)
    query="CREATE (node:computer_hardware{})"
    sess.run(query)
    query = "MATCH (a:Records),(b:others) WHERE a.Item <> 'mouse' AND a.Item <> 'Computer' AND a.Item <> 'LED lights' CREATE (a)-[r: belongs_to]->(b)"
    sess.run(query)
    query="MATCH (a:Records),(b:computer_hardware) WHERE a.Item = 'mouse' OR a.Item = 'Computer' CREATE (a)-[r: belongs_to]->(b)"
    sess.run(query)
    query="MATCH (a:Records),(b:lights) WHERE a.Item = 'LED lights' CREATE (a)-[r: belongs_to]->(b)"
    sess.run(query)
# relations()





def Retrieve_from_db():
    dbcon = GraphDatabase.driver(uri="neo4j://localhost:7687", auth=("neo4j", "root"))
    sess = dbcon.session()
    query= "Match (n) Return n"
    all_nodes= sess.run(query)
    for node in all_nodes:
        print(node)
Retrieve_from_db()
