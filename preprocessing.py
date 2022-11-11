"""
Contains code for reading inputs and any preprocessing necessary to make our algorithm work, and our database connection
"""
import psycopg2
import queue
from configparser import ConfigParser


class Database:
  def __init__(self, host="localhost", port=5432, database="TPC-H", user="postgres", password="database"):
    # Connect to postgres database called TPC-H
    self.conn = psycopg2.connect(host=host, port=port, database=database, user=user, password=password)
    # Create a cursor to perform database operations
    self.cur = self.conn.cursor()

  # Method for executing query and returning the results
  def execute(self,query):
    self.cur.execute(query)
    query_results = self.cur.fetchall()
    return query_results

  def close(self):
    self.cur.close()
    self.conn.close()


class Node(object):    
  """
  Node class defines the structure of the data that an individual node of a query plan can store and the functions that can be performed on it
  """
  def __init__(self, node_type, relation_name, schema, alias, group_key, sort_key, join_type, index_name, 
              hash_condition, table_filter, index_condition, merge_condition, recheck_condition, join_filter, subplan_name, actual_rows,
              actual_time, description):
    self.node_type = node_type
    self.children = []
    self.relation_name = relation_name
    self.schema = schema
    self.alias = alias
    self.group_key = group_key
    self.sort_key = sort_key
    self.join_type = join_type
    self.index_name = index_name
    self.hash_condition = hash_condition
    self.table_filter = table_filter
    self.index_condition = index_condition
    self.merge_condition = merge_condition
    self.recheck_condition = recheck_condition
    self.join_filter = join_filter
    self.subplan_name = subplan_name
    self.actual_rows = actual_rows
    self.actual_time = actual_time
    self.description = description

  def append_children(self, child):
    self.children.append(child)
  
  def write_qep_output_name(self, output_name):
    if "T" == output_name[0] and output_name[1:].isdigit():
      self.output_name = int(output_name[1:])
    else:
      self.output_name = output_name

  def read_qep_output_name(self):
    if str(self.output_name).isdigit():
      return "T" + str(self.output_name)
    else:
      return self.output_name

  def set_step(self, step):
    self.step = step
  
  def update_desc(self,desc):
    self.description = desc


class QueryPlans:
  def __init__(self, sql_query):
    self.connection = Database("localhost", 5432, "postgres", "postgres", "password")
    self.sql_query = sql_query
  
  def generateQEP(self):
    return self.connection.execute(f"EXPLAIN (format json) {self.sql_query}")

  def generateAQPs(self, qep_node_types):
    aqps = []
    prev_condition = None
    for nt in qep_node_types:
      if nt == "Nested Loop":
        condition = "enable_nestloop"
      elif nt == "Seq Scan":
        condition = "enable_seqscan"
      elif nt == "Index Scan":
        condition = "enable_indexscan"
      elif nt == "Bitmap Index Scan" or nt == "Bitmap Heap Scan":
        condition = "enable_bitmapscan"
      elif nt == "Hash Join":
        condition = "enable_hashjoin"
      elif nt == "Merge Join":
        condition = "enable_mergejoin"
      if prev_condition:
        aqps.append(self.connection.execute(f"set {prev_condition} = 'on'; set {condition} = 'off'; EXPLAIN (format json) {self.sql_query}"))
      else:
        aqps.append(self.connection.execute(f"set {condition} = 'off'; EXPLAIN (format json) {self.sql_query}"))
      prev_condition = condition
    return aqps

  def extract_qp_data(self, json_obj):
    """
    This function parses the QP in JSON format and stores the attributes generated by the EXPLAIN query
    Args:
        qep_json_obj (json): QP generated from EXPLAIN (FORMAT JSON) query
    Returns:
        root_node (Node): Returns the root node of the generated QP tree.
        node_types (set): all node types used in the QP
    """

    # q_child_plans : Queue to store all child plans
    # q_parent_plans : Queue to store parents of all child plans
    q_child_plans = queue.Queue()
    q_parent_plans = queue.Queue()
    plan = json_obj[0]['Plan']
    q_child_plans.put(plan)
    q_parent_plans.put(None)
    node_types = set()

    while not q_child_plans.empty():
      current_plan = q_child_plans.get()
      parent_node = q_parent_plans.get()

      relation_name = schema = alias = group_key = sort_key = join_type = index_name = hash_condition = table_filter \
        = index_condition = merge_condition = recheck_condition = join_filter = subplan_name = actual_rows = actual_time = description = None
      if 'Relation Name' in current_plan:
          relation_name = current_plan['Relation Name']
      if 'Schema' in current_plan:
          schema = current_plan['Schema']
      if 'Alias' in current_plan:
          alias = current_plan['Alias']
      if 'Group Key' in current_plan:
          group_key = current_plan['Group Key']
      if 'Sort Key' in current_plan:
          sort_key = current_plan['Sort Key']
      if 'Join Type' in current_plan:
          join_type = current_plan['Join Type']
      if 'Index Name' in current_plan:
          index_name = current_plan['Index Name']
      if 'Hash Cond' in current_plan:
          hash_condition = current_plan['Hash Cond']
      if 'Filter' in current_plan:
          table_filter = current_plan['Filter']
      if 'Index Cond' in current_plan:
          index_condition = current_plan['Index Cond']
      if 'Merge Cond' in current_plan:
          merge_condition = current_plan['Merge Cond']
      if 'Recheck Cond' in current_plan:
          recheck_condition = current_plan['Recheck Cond']
      if 'Join Filter' in current_plan:
          join_filter = current_plan['Join Filter']
      if 'Actual Rows' in current_plan:
          actual_rows = current_plan['Actual Rows']
      if 'Actual Total Time' in current_plan:
          actual_time = current_plan['Actual Total Time']
      if 'Subplan Name' in current_plan:
        if "returns" in current_plan['Subplan Name']:
          name = current_plan['Subplan Name']
          subplan_name = name[name.index("$"):-1]
        else:
          subplan_name = current_plan['Subplan Name']
      # form a node form attributes created above
      current_node = Node(current_plan['Node Type'], relation_name, schema, alias, group_key, sort_key, join_type,
                          index_name, hash_condition, table_filter, index_condition, merge_condition, recheck_condition, join_filter,
                          subplan_name, actual_rows, actual_time, description)
      node_types.add(current_node.node_type)
      # Parse for the various nodetypes. Some of the most common ones are : Seq Scan, Index Only Scan, 
      # Index Scan, Bitmap Index/Heap Scan, Limit, Sort and Nested Loop. 
        
      if current_node.node_type == "Limit":
        current_node.plan_rows = current_plan['Plan Rows']

      if "Scan" in current_node.node_type:
        if "Index" in current_node.node_type:
          if relation_name:
            current_node.write_qep_output_name("{} with index {}".format(relation_name, index_name))
        elif "Subquery" in current_node.node_type:
          current_node.write_qep_output_name(alias)
        else:
          current_node.write_qep_output_name(relation_name)

      if parent_node:
        parent_node.append_children(current_node)
      else:
        root_node = current_node

      if 'Plans' in current_plan:
        for item in current_plan['Plans']:
          # push child plans into queue
          q_child_plans.put(item)
          # push parent for each child into queue
          q_parent_plans.put(current_node)

    return root_node, node_types

def config(filename='database.ini', section='postgresql'):
  # create a parser
  parser = ConfigParser()
  # read config file
  parser.read(filename)

  # get section, default to postgresql
  db = {}
  if parser.has_section(section):
    params = parser.items(section)
    for param in params:
      db[param[0]] = param[1]
  else:
    raise Exception('Section {0} not found in the {1} file'.format(section, filename))

  return db


def connect():
  """ Connect to the PostgreSQL database server and print version number"""
  conn = None
  try:
    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)

    # create a cursor
    cur = conn.cursor()

    # execute a statement
    print('PostgreSQL database version:')
    cur.execute('SELECT version()')

    # display the PostgreSQL database server version
    db_version = cur.fetchone()
    print(db_version)

    # close the communication with the PostgreSQL
    cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')


def runQuery(query: str) -> list:
  """ Runs the query on the PostgreSQL database server and returns the result as a matrix

  Returns
  ------
  ['Connection Error'] if there is a connection error with the database.
  Query results in the form of a matrix otherwise """
  conn = None
  try:
    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)

    # create a cursor
    cur = conn.cursor()

    # execute a statement
    print('PostgreSQL database version:')
    cur.execute(query)

    # display the result
    result = []
    while True:
      nextLine = cur.fetchone()
      if (nextLine is None):
        break
      result.append(list(nextLine))

      # close the communication with the PostgreSQL
    cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')
  try:
    return result
  except:
    # raise Exception('Couldn\'t connect to database')
    return ['Connection Error']


def explainQuery(query: str, format='text'):
  """ Explains the query on the PostgreSQL database server and returns the result as a matrix

  Returns
  ------
  'Connection Error' if there is a connection error with the database.
   Query results in the form of a matrix otherwise

  Input
  -----
  query - The query to be explained as a string. Please dont include the EXPLAIN keyword in the query.
   format - Either 'text' or 'json'. Default is text. """
  # Adding Explain Keyword
  if (format == 'json'):
    query = "EXPLAIN (format json) " + query
  else:
    query = "EXPLAIN " + query
    format = 'text'

  # Connecting to DB and running
  conn = None
  try:
    # read connection parameters
    params = config()

    # connect to the PostgreSQL server
    print('Connecting to the PostgreSQL database...')
    conn = psycopg2.connect(**params)

    # create a cursor
    cur = conn.cursor()

    # execute a statement
    print('PostgreSQL database version:')
    cur.execute(query)

    # display the result
    if (format == 'text'):
      result = ''
      while True:
        nextLine = cur.fetchone()
        if (nextLine is None):
          break
        result += nextLine[0] + '\n'
    # display the result
    elif (format == 'json'):
      result = cur.fetchone()[0]

      # close the communication with the PostgreSQL
    cur.close()
  except (Exception, psycopg2.DatabaseError) as error:
    print(error)
  finally:
    if conn is not None:
      conn.close()
      print('Database connection closed.')
  try:
    return result
  except:
    # raise Exception('Couldn\'t connect to database')
    return ['Connection Error']

def stringProcess(list_nodes, col_level = []):
    # Adjusting list_nodes such that each element is a single node in a graph
    i = 1
    while(i < len(list_nodes)):
        # If an element doesn't have -> in it then it is not a node by itself but a part of the previous node
        if('->' not in list_nodes[i]):
            # Merge with previous string
            list_nodes[i-1] = list_nodes[i-1] + '\n' +  list_nodes[i].lstrip()
            list_nodes.pop(i)
        else:
            i += 1

    # Finding the indent level of each node
    indent_level = []
    i = 0
    while(i < len(list_nodes)):
        indent_level.append(len(list_nodes[i]) - len(list_nodes[i].lstrip()))
        i += 1

    # print(indent_level)

    # Finding the column level of each node
    col_level = []
    curr_col_level = 0
    i = 0
    while(i < len(list_nodes)):
        # Checking if there is a node ahead at the same indent level
        j = i+1
        while(j < len(list_nodes) and indent_level[j] > indent_level[i]):
            j += 1
        if(j != len(list_nodes)) and (indent_level[j] == indent_level[i] and j != i):
            # There exits another node at same indent level
            curr_col_level += 1
            col_level.append(curr_col_level)
        else:
            # There isnt a node ahead so we check behind
            j = i-1
            while(j >= 0):
                if(indent_level[j] == indent_level[i]):
                    break
                j -= 1
            # If there is a node behind
            if(indent_level[i] == indent_level[j] and j >= 0):
                curr_col_level = col_level[j] - 1
                col_level.append(curr_col_level)
            else:
                col_level.append(curr_col_level)
        # Cleaning of text (Removing extra spaces and -> from the strings)
        if(i != 0):
            list_nodes[i] = list_nodes[i].lstrip()[2:].strip()
        i += 1 

    # print(col_level)

    # Making sure that there is always a new line char in every string (For arrow length consistancy)
    i = 0
    while(i < len(list_nodes)):
        if('\n' not in list_nodes[i]):
            try:
                index1 = list_nodes[i].index('(')
                list_nodes[i] = list_nodes[i][:index1] + '\n' + list_nodes[i][index1:]
            except:
                pass
        i += 1
    
    return list_nodes, col_level

