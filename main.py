#%%
from neo4j import GraphDatabase
import json

# load configuration settings from a JSON file. I
# If you are using a local installation,
# use data/config.json
with open('configs/config_aura.json', 'r') as conf: 
    config = json.load(conf)

# set up the connection parameters
# from the loaded configuration.
URI = config['URI']
AUTH = (config['DB'], config['PASSWORD'])  # credentials are set in config.json
DOC = config['DATA']  # path to the data file is specified in config.json
driver = GraphDatabase.driver(URI, auth=AUTH)

#%%

def create_node(driver, node_name :str):
    _cql = f'CREATE ({node_name})'
    print(_cql)
    with driver.session() as session:
        session.run(_cql)

def create_nodes(driver, node_names : list = []):
    
    _node_names = ['('+n+')' for n in node_names]
    _node_names = ','.join(_node_names)    
    _cql = f'CREATE {_node_names}'
    print(_cql)
    with driver.session() as session:
        session.run(_cql)

def create_node_label(
    driver,
    node_name : str,
    node_label : str
    ):
    _cql = f'CREATE ({node_name}:{node_label})'
    print(_cql)
    with driver.session() as session:
        session.run(_cql)
        
def create_node_label_props(
    driver,
    node_name : str,
    node_label : str,
    props : dict
    ):
    _props = []
    for k in props.keys():
        if isinstance(props[k], str):
            _props.append(k+':'+'\"' +props[k]+ '\"')
        else:
             _props.append(k+':'+str(props[k]))
    _props = ', '.join(_props)     
    _cql = f'CREATE ({node_name}:{node_label} '+ '{' + _props + '})'
    print(_cql)
    with driver.session() as session:
        session.run(_cql)


def create_relationship(type_first_node : str, 
                        type_second_node : str, 
                        prop_name_first_node : str, 
                        prop_name_second_node: str,
                        type_rel: str = 'IS_RELATED_TO'
                        ):
    _cql = f"MATCH (a:{type_first_node}), (b:{type_second_node}) WHERE a.name = \"{prop_name_first_node}\" AND b.name = \"{prop_name_second_node}\" " + \
            f"CREATE (a)-[r: {type_rel}]->(b)"
    print(_cql)
    with driver.session() as session:
        session.run(_cql)
#%%
# create_node(driver, 'movie0') 
create_nodes(driver, ['n0', 'n1'])

#%%
create_node_label(driver, 'n0', 'Person')
#%%
create_node_label_props(driver, 'n0', 'Person', {'name':'Alice', 'weight':'60kg', 'age':28})
#%%
create_node_label_props(driver, 'n1', 'Person', {'name':'Giulio', 'weight':'80kg', 'age':30})
#%%
create_relationship('Person', 'Person', 'Alice', 'Giulio')
#%%
create_relationship('Person', 'Person', 'Alice', 'Giulio', 'IS_FRIEND_OF')
#%%

# match (n:Person) return n
# match (n) where n.name = 'Alice' return n
# match (n) where n.name CONTAINS 'i' return n
# match (n) where n.age > 20 return n