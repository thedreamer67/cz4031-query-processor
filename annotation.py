"""
Contains code for generating the annotations
"""

# Compares 2 diff query plans and returns the differences as a string
def compare_two_plans(root_node_qep, root_node_aqp):
  global diff_idx
  reset_vars()
  convert_qp_to_text(root_node_aqp)
  # print('description:', root_node_aqp.description)
  # print("AQP Steps:", steps)

  reset_vars()
  convert_qp_to_text(root_node_qep)
  # print("QEP Steps:", steps)

  diff_idx=1
  difference = []
  reasons = []
  compare_children_nodes(root_node_aqp, root_node_qep, difference, reasons) # returns list of difference
    
  diff_str = ""
  for i in range(len(reasons)):
    diff_str += difference[i] + "\n"
    if reasons[i] != "":
      diff_str = diff_str + reasons[i] + "\n"

  return diff_str


def convert_qp_to_text(node, skip=False):
  """
  This function converts the QP node to text, based on a set of rules which have been pre-determined
  Args:
      node (Node): Node of the QP
      skip (bool, optional): Skip processing of current node (default is false)
  """

  global steps, cur_step, cur_table_name
  increment = True
  # skip the child if merge it with current node

  if node.node_type in ["Unique", "Aggregate"] and len(node.children) == 1 and ("Scan" in node.children[0].node_type or node.children[0].node_type == "Sort"):
    children_skip = True
  elif node.node_type == "Bitmap Heap Scan" and node.children[0].node_type == "Bitmap Index Scan":
    children_skip = True
  else:
    children_skip = False

  # recursive
  for child in node.children:
    if node.node_type == "Aggregate" and len(node.children) > 1 and child.node_type == "Sort":
      convert_qp_to_text(child, True)
    else:
      convert_qp_to_text(child, children_skip)

  if node.node_type in ["Hash"] or skip:
    return

  step=""
  # Extract textual translation of QEP.
  # Combining hash with hash join under certain condition which can be extracted. 
  if "Join" in node.node_type: #if current node is a join operator
    if node.join_type == "Semi":
      node_type_list = node.node_type.split() #extract node_type into an array
      node_type_list.insert(-1, node.join_type) #insert "Semi" at last element
      node.node_type = " ".join(node_type_list) #convert array back to string
    else:
      pass

    if "Hash" in node.node_type: #if current node is a hash join
      step += "and perform {} on ".format(node.node_type.lower()) #decapitalise
      for i, child in enumerate(node.children):
        if child.node_type == "Hash":
          child.write_qep_output_name(child.children[0].read_qep_output_name()) #extract the name of the table pass into hash operator
          hashed_table = child.read_qep_output_name() #extract the name of the table being hashed
        if i < len(node.children) - 1: #not last child
          step += ("table {} ".format(child.read_qep_output_name())) #extracting name of table
        else: #last child
          step+= (" and table {}".format(child.read_qep_output_name()))
      step = "hash table {} {} under condition {}".format(hashed_table, step, extract_qep_conditions("Hash Cond", node.hash_condition, table_subquery_name_pair))
        #i.e. hash the table (hashed table) and perform join on (listing all child tables) under condition of (hash condition)

    elif "Merge" in node.node_type: #if current node is merge join
      step += "perform {} on ".format(node.node_type.lower())
      any_sort = False  # Flag to indicate if sorting has been performed on child
      for i, child in enumerate(node.children):
        if child.node_type == "Sort": #child operator was a sort operation. hence table has been sorted
          child.write_qep_output_name(child.children[0].read_qep_output_name()) #update child's output name with the name of the table pass into sort operator
          any_sort = True #indicate that sorting has been done
        if i < len(node.children) - 1: #not last table
          step += ("table {} ".format(child.read_qep_output_name()))
        else: #last table
          step += (" and table {} ".format(child.read_qep_output_name()))
      # combining sort with merge if table has been sorted
      if any_sort: #some tables have been sorted
        sort_step = "sort "
        for child in node.children:
          if child.node_type == "Sort": #find which tables have been sorted and insert into sort step
            if i < len(node.children) - 1: ##not last table
              sort_step += ("table {} ".format(child.read_qep_output_name()))
            else:
              sort_step += (" and table {} ".format(child.read_qep_output_name()))

        step = "{} and {}".format(sort_step, step) #if any_sort == false, then no step will just remain as is without the sort_step part added

  elif node.node_type == "Bitmap Heap Scan":
    # combine bitmap heap scan and bitmap index scan
    if "Bitmap Index Scan" in node.children[0].node_type:
      node.children[0].write_qep_output_name(node.relation_name)
      step = " with index condition {} ".format(extract_qep_conditions("Recheck Cond", node.recheck_condition,table_subquery_name_pair))
      ## extract index condition

    step = "perform bitmap heap scan on table {} {} ".format(node.children[0].read_qep_output_name(), step) #join the bitmap heapscan with table name and index condition

  elif "Scan" in node.node_type: #node_type is a string
    if node.node_type == "Seq Scan":
      step += "perform sequential scan on table " #extend the short form for better readability
    else:
      step += "perform {} on table ".format(node.node_type.lower())

    step += node.read_qep_output_name() #extract table name, the scan was perform on

    if not node.table_filter:
      increment = False

  elif node.node_type == "Unique": #extracting unique values from table
    # combine unique and sort
    if "Sort" in node.children[0].node_type: #unique operator has child sort operator
      node.children[0].write_qep_output_name(
        node.children[0].children[0].read_qep_output_name()) #extract the table name before sort
      step = "sort {} ".format(node.children[0].read_qep_output_name()) #extract the table name to be sorted
      if node.children[0].sort_key: #key value the table is sorted on
        step += "with attribute {} and ".format(extract_qep_conditions("Sort Key", node.children[0].sort_key, table_subquery_name_pair))
      else: #no sort key
        step += " and "
    step += "select unique tuples from table {} ".format(node.children[0].read_qep_output_name())

  elif node.node_type == "Aggregate": #max, sum, avg etc.
    for child in node.children:
      # combine aggregate and sort
      if "Sort" in child.node_type:
        child.write_qep_output_name(child.children[0].read_qep_output_name()) #extract name of table for sorting
        step = "sort {} and ".format(child.read_qep_output_name())
      # combine aggregate and scan
      if "Scan" in child.node_type:
        if child.node_type == "Seq Scan":
          step = "perform sequential scan on {} and ".format(child.read_qep_output_name())
        else:
          step = "perform {} on {} and ".format(child.node_type.lower(), child.read_qep_output_name())
    
    step += "perform aggregate on table {}".format(node.children[0].read_qep_output_name())
    ## IMPROVEMENT: is there a way to find which aggregate function is used, and which column is used

    if len(node.children) == 2: #max of 2 children for aggregate function, BUT WHY?
      step += " and table {} ".format(node.children[1].read_qep_output_name())

  elif node.node_type == "Sort":
    step+= "perform sort on table {} with {}".format(node.children[0].read_qep_output_name(), extract_qep_conditions("Sort Key", node.sort_key, table_subquery_name_pair))

  elif node.node_type == "Limit": #how many rows to extract
    step += "limit the result from table {} to {} record(s)".format(node.children[0].read_qep_output_name(), node.plan_rows)
  
  else:
    step += "perform {} on ".format(node.node_type.lower())
    if len(node.children) > 1: #more than one child
      for i, child in enumerate(node.children):
        if i < len(node.children) - 1:
          step += (" table {},".format(child.read_qep_output_name()))
        else:
          step += (" and table {} ".format(child.read_qep_output_name()))
    else: #only one child
      step+= " table {}".format(node.children[0].read_qep_output_name())
  
  if node.group_key: #if there is a "group_by"
    step += " with grouping on attribute {}".format(extract_qep_conditions("Group Key", node.group_key, table_subquery_name_pair))
  if node.table_filter:
    step += " and filtering on {}".format(extract_qep_conditions("Table Filter", node.table_filter, table_subquery_name_pair))
  if node.join_filter:
    step += " while filtering on {}".format(extract_qep_conditions("Join Filter", node.join_filter, table_subquery_name_pair))

  if increment: #current node is a intermediate table
    node.write_qep_output_name("T" + str(cur_table_name))  #renaming to T1, T2 for intermediate table
    step += " to get intermediate table " + node.read_qep_output_name()
    cur_table_name += 1
  if node.subplan_name:
    table_subquery_name_pair[node.subplan_name] = node.read_qep_output_name()

  node.update_desc(step) #shows what is being done at this node
  step = "\nStep {}, {}.".format(cur_step, step) #join step description with Step count
  node.set_step(cur_step) #what step count is the cur step at
  cur_step += 1

  steps.append(step) #recursive function, return control to parent node


def extract_qep_conditions(op_name, conditions, table_subquery_name_pair):
  """
  Args:
      op_name (string): Name of the operation - Sort, Join, etc.
      conditions (dynamic): Attribute to filter upon. This can be a key or a operation condition of a node
      table_subquery_name_pair ([type]): [description]
  Returns:
      string : Explanation for the condition to be met
  """

  if isinstance(conditions, str): #if conditions is string type
    if "::" in conditions:
      return conditions.replace("::", " ")[1:-1] #replace with spaces
    return conditions[1:-1]
  cond = ""
  for i in range(len(conditions)): #if conditions is an array of strings
    cond = cond + conditions[i] #add to cond string
    if (not (i == len(conditions) - 1)): #not at the last element
      cond = cond + "and"
  return cond


def compare_children_nodes(nodeAQP, nodeQEP, difference, reasons):
  """
  This function recursively traveses both plan trees and compares the corresponding nodes 
  Args:
      nodeAQP (Node): AQP node
      nodeQEP (Node): QEP node
      difference (string): Structural difference between two nodes
      reasons (string): Explanation for difference between the nodes
  """
  # print(f"comparing children nodes AQP.{nodeAQP.node_type} and QEP.{nodeQEP.node_type}")
  global diff_idx #keeps track of the count of differences found
  childrenA = nodeAQP.children #list of children node
  childrenB = nodeQEP.children
  children_no_A = len(childrenA)
  children_no_B = len(childrenB)

  # If both node_types are the same and they have the same number of children (!= 0), compare each corresponding pair of children
  if nodeAQP.node_type == nodeQEP.node_type and children_no_A == children_no_B: #no difference between node of AQP and node of QEP
    if children_no_A > 0: #not no children
      for i in range(len(childrenA)):
        compare_children_nodes(childrenA[i], childrenB[i], difference, reasons)

  else: #there is a difference between node of AQP and node of QEP
    # If AQP node_type == "Hash" or "Sort"
    if nodeAQP.node_type == 'Hash' or nodeAQP.node_type == 'Sort': #compare the child of AQP (being hashed or sorted) with current node of QEP
        #since they are different, there's no longer a need to Hash or Sort, instead it is changed to current node of QEP
      text = f"Difference {diff_idx}: {nodeAQP.children[0].description} has been transformed to {nodeQEP.description}"

      text = modify_text(text)
      difference.append(text)
      reason = generate_node_diff_reason(nodeAQP.children[0], nodeQEP, diff_idx)
      reasons.append(reason)
      diff_idx += 1

    # If QEP node_type == "Hash" or "Sort", and AQP is not Hashing and Sorting
    elif nodeQEP.node_type == 'Hash' or nodeQEP.node_type == 'Sort':
      text = f"Difference {diff_idx}: {nodeAQP.description} has been transformed to {nodeQEP.children[0].description}"

      text = modify_text(text)
      difference.append(text)
      reason = generate_node_diff_reason(nodeAQP, nodeQEP.children[0], diff_idx)
      reasons.append(reason)
      diff_idx += 1

    elif 'Gather' in nodeAQP.node_type:
      compare_children_nodes(childrenA[0], nodeQEP, difference, reasons)

    elif 'Gather' in nodeQEP.node_type:
      compare_children_nodes(nodeAQP, childrenB[0], difference, reasons)
      
    else:
      text = f"Difference {diff_idx}: {nodeAQP.description} has been transformed to {nodeQEP.description}"
      text = modify_text(text)
      difference.append(text)
      reason = generate_node_diff_reason(nodeAQP, nodeQEP, diff_idx)
      reasons.append(reason)
      diff_idx += 1

    if children_no_A == children_no_B:
      if children_no_A == 1:
        compare_children_nodes(childrenA[0], childrenB[0], difference, reasons)
      if children_no_A == 2:
        compare_children_nodes(childrenA[0], childrenB[0], difference, reasons)
        compare_children_nodes(childrenA[1], childrenB[1], difference, reasons)

  return


def modify_text(text):
  text = text.replace('perform ', '')
  return text


def generate_node_diff_reason(node_aqp, node_qep, diff_idx):
    """
    This function generates the reasons for the difference in the nodes of the 2 QPs being compared.
    Args:
        node_aqp (Node): AQP node
        node_qep (Node): QEP node
        diff_idx (int): Index of the difference for which the reason is being generated
    Returns:
        string : Reason for difference between AQP and QEP nodes
    """
    text = ""

  # If AQP node == Index Scan and QEP node == Seq Scan
    # if node_aqp.node_type == "Index Scan" and node_qep.node_type == "Seq Scan":
    #     text = f"Difference {diff_idx} Reasoning: "
    #     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to Sequential Scan in QEP on relation {node_qep.relation_name}. "
    #     # check conditions for transformation from end condition. Here seq scan doesn't use index
    #     if node_qep.index_name is None:
    #         text += f"AQP uses the index attribute {node_aqp.index_name} for selection, which is not used by QEP" #index scan can only be done when an index is present
    #     if int(node_aqp.actual_rows) < int(node_qep.actual_rows):
    #         text += f"and because of this, the actual row count returned increases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "

    #     if node_aqp.index_condition != node_qep.table_filter and int(node_aqp.actual_rows) < int(node_qep.actual_rows):
    #         text += "This behavior is generally consistent with the change in the selection predicates from {} to {}.".format(node_aqp.index_condition if node_aqp.index_condition is not None else "None", node_qep.table_filter if node_qep.table_filter is not None else "None")
    #         #CHANGES: need change the last text, maybe talk about having smaller return table is better.


    #NEW CODE#
    #Index Scan vs Seq Scan vs Bitmap Scan
    #6 Combinations Total
    #Case 1: Index Scan over Seq Scan
    if node_qep.node_type == "Index Scan" and node_aqp.node_type == "Seq Scan":
        text = f"Difference {diff_idx} Reasoning: "
        text += f"Sequential Scan in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        text += "Sequential Scan to scans over the entire table, which is the most inefficient out of the three algorithms. "
        if node_qep.index_condition == node_qep.index_name:
            text += "Since an index on the filter attribute is present for the scan, index scan will cost less and hence was chosen over sequential scan."

    #Case 2: Seq Scan over Index Scan
    elif node_aqp.node_type == "Index Scan" and node_qep.node_type == "Seq Scan":
        text = f"Difference {diff_idx} Reasoning: "
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to Sequential Scan in QEP on relation {node_qep.relation_name}. "
        text += "Given the higher per row cost of index scan and the low selectivity of the scan predicate, sequential scan becomes more cost effective when compared to index scan. \
            Hence, sequential scan is chosen over index scan."

    #Case 3: Index Scan over Bitmap Scan
    elif node_qep.node_type == "Index Scan" and "Bitmap" in node_aqp.node_type and "Scan" in node_aqp.node_type:
        text = f"Difference {diff_idx} Reasoning: "
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to Index Scan in QEP on relation {node_qep.relation_name}. "
        text += f"Since the scan predicate, {node_qep.index_condition}, has a high selectivity. It is more efficient to perform index scan as compared to bitmap scan."

    #Case 4: Bitmap Scan over Index Scan
    elif node_aqp.node_type == "Index Scan" and "Bitmap" in node_qep.node_type and "Scan" in node_qep.node_type:
        text = f"Difference {diff_idx} Reasoning: "
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        text += f"Since the scan predicate, {node_qep.index_condition}, has a low selectivity. In order to take advantage of the ease of bulk data reading, bitmap scan is chosen over index scan."
    
    #Case 5: Seq Scan over Bitmap Scan
    elif node_qep.node_type == "Seq Scan" and "Bitmap" in node_aqp.node_type and "Scan" in node_aqp.node_type:
        text = f"Difference {diff_idx} Reasoning: "
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to Sequential Scan in QEP on relation {node_qep.relation_name}."
        text += "Sequential scan is chosen over bitmap scan when the bitmap does not fit in the working memory. "
        text += "Hence, using bitmap scan will incur additional I/O for the bitmap, making sequential scan more cost efficient."

    #Case 6: Bitmap Scan over Seq Scan
    elif node_aqp.node_type == "Seq Scan" and "Bitmap" in node_qep.node_type and "Scan" in node_qep.node_type:
        text = f"Difference {diff_idx} Reasoning: "
        text += f"Seq Scan in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}."
        text += f"Bitmap scan takes advantage of the index on relation {node_qep.relation_name} to reduce the scan space, hence it is more cost efficient compared to sequential scan."

#   # If AQP node == Seq Scan and QEP node == Index Scan
#   elif node_qep.node_type == "Index Scan" and node_aqp.node_type == "Seq Scan":
#     text = f"Difference {diff_idx} Reasoning: "
#     text += f"Sequential Scan in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
#     if node_aqp.index_name is None:  
#       text += f"QEP uses the index attribute {node_qep.index_name} for selection, which is not used by AQP. "
#     elif node_aqp.index_name is not None:
#       text += f"Both AQP and QEP use their index attributes for selection, which are {node_aqp.index_name} and {node_qep.index_name} respectively. "
#     if int(node_aqp.actual_rows) > int(node_qep.actual_rows):
#       text += f"As such, the actual row count returned decreases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
#     if node_aqp.table_filter != node_qep.index_condition and int(node_aqp.actual_rows) > int(node_qep.actual_rows):
#       text += "This behavior is generally consistent with the change in the selection predicates from {} to {}.".format(node_aqp.table_filter if node_aqp.table_filter is not None else "None", node_qep.index_condition if node_qep.index_condition is not None else "None")

#     # FIND OUT ABOUT INDEX SCAN vs BITMAP SCAN vs SEQ SCAN
#   # If AQP node == Index Scan and QEP node == Bitmap Scan
#   elif node_aqp.node_type == "Index Scan" and "Bitmap" in node_qep.node_type and "Scan" in node_qep.node_type:
#     text = f"Difference {diff_idx} Reasoning: "
#     text += f"Index Scan in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. This can be attributed to the fact that large amount of data must be read, so PostgreSQL decided to use {node_qep.node_type} which is more efficient in reading a larger number of records, when compared to Index Scan."

#   # If QEP node == Index Scan and AQP node == Bitmap Scan
#   elif node_qep.node_type == "Index Scan" and "Bitmap" in node_aqp.node_type and "Scan" in node_aqp.node_type:
#     text = f"Difference {diff_idx} Reasoning: "
#     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to Index Scan in QEP on relation {node_qep.relation_name}. This can be attributed to the fact that only a small amount of data must be read, so PostgreSQL decided to use Index Scan which is more efficient in reading a smaller number of records."

#   # If AQP node == Seq Scan and QEP node == Bitmap Scan
#   elif node_aqp.node_type == "Seq Scan" and "Bitmap" in node_qep.node_type and "Scan" in node_qep.node_type:
#     text = f"Difference {diff_idx} Reasoning: "
#     text += f"Seq Scan in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. This can be attributed to the fact that a small amount of data must be read, so PostgreSQL decided to use {node_qep.node_type} which is more efficient in reading a smaller number of records than Seq Scan."

#   # If QEP node == Seq Scan and AQP node == Bitmap Scan
#   elif node_qep.node_type == "Seq Scan" and "Bitmap" in node_aqp.node_type and "Scan" in node_aqp.node_type:
#     text = f"Difference {diff_idx} Reasoning: "
#     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to Seq Scan in QEP on relation {node_qep.relation_name}. This can be attributed to the fact that a large amount of data must be read, so PostgreSQL decided to use Seq Scan which is more efficient in reading a large number of records."

  ##NEW CODE##
  # If nodes are join nodes
   # "Merge Join" vs "Hash Join" vs "Nested Loop"
    elif node_aqp.node_type and node_qep.node_type in ['Merge Join', "Hash Join", "Nested Loop"]:
      text = f"Difference {diff_idx} Reasoning: "
      #case #1: Merge Join over nested loop
      if node_aqp.node_type == "Nested Loop" and node_qep.node_type == "Merge Join":
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        if "=" in node_qep.node_type:
          text += "The join condition is performed with an equality operator. "
        text += "Nested Loop is generally considered the most inefficient out of the three algorithms. "
        text += "Since the relations to be joined are already sorted, merge join is more efficient and hence is chosen over nested loop. "
   

      #case #2: nested loop over merge join
      if node_aqp.node_type == "Merge Join" and node_qep.node_type == "Nested Loop":
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        if "=" in node_qep.node_type:
          text += "The join condition is performed with an equality operator. "
        else:
          text += "The join condition is not an equality operator, hence Nested Loop Join is more suitable. "
        text += "Given that the outer loop relation is relatively small and all tuples with the same join attribute values cannot fit in memory, doing nested loop join will be more cost efficient than merge join. "

      #case #3: merge Join over Hash Join
      if node_aqp.node_type == "Hash Join" and node_qep.node_type == "Merge Join":
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        text += "Given that the hash table does not fit in the memory, hash join becomes dramatically slower as several loops are required. "
        text += "Hence, merge join is chosen over hash join."

      #case #4: hash join over merge join
      if node_aqp.node_type == "Merge Join" and node_qep.node_type == "Hash Join":
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        text += "Hash table can fit in memory, which lowers the cost of hash join. In addition, sorting the operands first will result in a higher cost. Hence, hash join is chosen over merge join."
      #case #5: Hash Join over nested loop
      if node_aqp.node_type == "Nested Loop" and node_qep.node_type == "Hash Join":
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        if "=" in node_qep.node_type:
          text += "The join condition is performed with an equality operator. Hence, nested loop join is the least efficient out of the three algorithm. "
        text += "Hash table can fit in memory, which lowers the cost of hash join. Hence, hash join is chosen over nested loop join."

      #case #6: nested loop over hash join
      if node_aqp.node_type == "Hash Join" and node_qep.node_type == "Nested Loop":
        text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
        if "=" in node_qep.node_type:
          text += "The join condition is performed with an equality operator. "
        else:
          text += "The join condition is not an equality operator, hence Nested Loop Join is more suitable. "
        text += "One of the operand has very few rows. Hence, nested loop join is more cost efficient, while saving on the set up cost."

  
  # elif node_aqp.node_type and node_qep.node_type in ['Merge Join', "Hash Join", "Nested Loop"]:
  #   text = f"Difference {diff_idx} Reasoning: "

  #   # If AQP node == Nested Loop and QEP node == Merge Join
  #   # Merge Join involves sorting
  #   if node_aqp.node_type == "Nested Loop" and node_qep.node_type == "Merge Join":
  #     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
  #     if int(node_aqp.actual_rows) < int(node_qep.actual_rows):
  #       text += f"The actual row count returned increases from {node_aqp.actual_rows} to {node_qep.actual_rows}. " ## TO CHANGE: increase why still better
  #     if "=" in node_qep.node_type:
  #       text += "The join condition is performed with an equality operator. "
  #     text += "Both sides of the Join operator in QEP can be sorted on the join condition efficiently. "

  #   # If AQP node == Nested Loop and QEP node == Hash Join
  #   if node_aqp.node_type == "Nested Loop" and node_qep.node_type == "Hash Join":
  #     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
  #     if int(node_aqp.actual_rows) < int(node_qep.actual_rows):
  #       text += f"The actual row count returned increases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #     if "=" in node_qep.node_type:
  #       text += "The join condition is performed with an equality operator. "

  #   # If AQP node == Merge Join and QEP node == Nested Loop
  #   if node_aqp.node_type == "Merge Join" and node_qep.node_type == "Nested Loop":
  #     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
  #     if int(node_aqp.actual_rows) > int(node_qep.actual_rows):
  #       text += f"The actual row count returned decreases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #     elif int(node_aqp.actual_rows) < int(node_qep.actual_rows):
  #       text += f"The actual row count returned increases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #       text += f"{node_qep.node_type} joins are used in the scenario where the join conditions are not performed with the equality operator. "
    
  #   # If AQP node == Merge Join and QEP node == Hash Join
  #   if node_aqp.node_type == "Merge Join" and node_qep.node_type == "Hash Join":
  #     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
  #     if int(node_aqp.actual_rows) < int(node_qep.actual_rows):
  #       text += f"The actual row count returned increases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #     if int(node_aqp.actual_rows) > int(node_qep.actual_rows):
  #       text += f"The actual row count returned decreases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #     text += "Both sides of the Join operator in QEP can be sorted on the join condition efficiently. "

  #   # If AQP node == Hash Join and QEP node == Nested Loop
  #   if node_aqp.node_type == "Hash Join" and node_qep.node_type == "Nested Loop":
  #     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
  #     if int(node_aqp.actual_rows) > int(node_qep.actual_rows):
  #       text += f"The actual row count returned decreases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #     elif int(node_aqp.actual_rows) < int(node_qep.actual_rows):
  #       text += f"The actual row count returned increases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #       text += f"{node_qep.node_type} joins are used in the scenario where the join conditions are not performed with the equality operator. "

  #   # If AQP node == Hash Join and QEP node == Merge Join
  #   if node_aqp.node_type == "Hash Join" and node_qep.node_type == "Merge Join":
  #     text += f"{node_aqp.node_type} in AQP on relation {node_aqp.relation_name} has now transformed to {node_qep.node_type} in QEP on relation {node_qep.relation_name}. "
  #     if int(node_aqp.actual_rows) < int(node_qep.actual_rows):
  #       text += f"The actual row count returned increases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #     if int(node_aqp.actual_rows) > int(node_qep.actual_rows):
  #       text += f"The actual row count returned decreases from {node_aqp.actual_rows} to {node_qep.actual_rows}. "
  #     text += "Both sides of the Join operator in QEP can be sorted on the join condition efficiently. "

  # return text


def reset_vars():
  """
  This function resets all the global variables to its default values so that a new QP can be used as input
  """
  global steps, cur_step, cur_table_name, table_subquery_name_pair
  steps = []
  cur_step = 1
  cur_table_name = 1
  table_subquery_name_pair = {}