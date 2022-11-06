"""
Contains code for generating the annotations
"""

import queue
import preprocessing
import json


# OLD
# def compare_two_plans(json_qep, json_aqp):
#   global diff_idx
#   qp = preprocessing.QueryPlans()
#   root_node_aqp, _ = qp.extract_qp_data(json_aqp)
#   reset_vars()
#   convert_qp_to_text(root_node_aqp)

#   root_node_qep, _ = qp.extract_qp_data(json_qep)
#   reset_vars()
#   convert_qp_to_text(root_node_qep)

#   diff_idx=1
#   difference = []
#   reasons = []
#   compare_children_nodes(root_node_aqp, root_node_qep, difference, reasons)
#   diff_str = ""
#   for i in range (len(reasons)):

#     diff_str = diff_str + difference[i] + "\n"
#     if reasons[i] != "":
#       diff_str = diff_str + reasons[i] + "\n"

#   return diff_str


def compare_two_plans(root_node_qep, root_node_aqp):
  global diff_idx
  reset_vars()
  convert_qp_to_text(root_node_aqp)
  # print('description:', root_node_aqp.description)
  print("AQP Steps:", steps)

  reset_vars()
  convert_qp_to_text(root_node_qep)
  print("QEP Steps:", steps)

  diff_idx=1
  difference = []
  reasons = []
  compare_children_nodes(root_node_aqp, root_node_qep, difference, reasons)

  diff_str = ""
  for i in range(len(reasons)):
    diff_str += difference[i] + "\n"
    if reasons[i] != "":
      diff_str = diff_str + reasons[i] + "\n"

  return diff_str





def convert_qp_to_text(node, skip=False):
  """
  This function converts the QEP node to text based on a set of rules, which have been predetermined.
  Args:
      node (Node): Node of the QEP
      skip (bool, optional): Skip processing of current node. Defaults to False.
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
  if "Join" in node.node_type:
    if node.join_type == "Semi":
      # Add 'semi' to 'join' incase of semi join 
      node_type_list = node.node_type.split()
      node_type_list.insert(-1, node.join_type)
      node.node_type = " ".join(node_type_list)
    else:
      pass

    if "Hash" in node.node_type:
      step += "and perform {} on ".format(node.node_type.lower())
      for i, child in enumerate(node.children):
        if child.node_type == "Hash":
          child.write_qep_output_name(child.children[0].read_qep_output_name())
          hashed_table = child.read_qep_output_name()
        if i < len(node.children) - 1:
          step += ("table {} ".format(child.read_qep_output_name()))
        else:
          step+= (" and table {}".format(child.read_qep_output_name()))
      step = "hash table {} {} under condition {}".format(hashed_table, step, extract_qep_conditions("Hash Cond", node.hash_condition, table_subquery_name_pair))



    elif "Merge" in node.node_type:
      step += "perform {} on ".format(node.node_type.lower())
      any_sort = False  # Flag indicated if sort has been performed on relation
      for i, child in enumerate(node.children):
        if child.node_type == "Sort":
          child.write_qep_output_name(child.children[0].read_qep_output_name())
          any_sort = True
        if i < len(node.children) - 1:
          step += ("table {} ".format(child.read_qep_output_name()))
        else:
          step += (" and table {} ".format(child.read_qep_output_name()))
      # combining sort with merge if table has been sorted
      if any_sort:
        sort_step = "sort "
        for child in node.children:
          if child.node_type == "Sort":
            if i < len(node.children) - 1:
              sort_step += ("table {} ".format(child.read_qep_output_name()))
            else:
              sort_step += (" and table {} ".format(child.read_qep_output_name()))

        step = "{} and {}".format(sort_step, step)

  elif node.node_type == "Bitmap Heap Scan":
    # combine bitmap heap scan and bitmap index scan
    if "Bitmap Index Scan" in node.children[0].node_type:
      node.children[0].write_qep_output_name(node.relation_name)
      step = " with index condition {} ".format(extract_qep_conditions("Recheck Cond", node.recheck_condition,table_subquery_name_pair))

    step = "perform bitmap heap scan on table {} {} ".format(node.children[0].read_qep_output_name(), step)


  elif "Scan" in node.node_type:
    if node.node_type == "Seq Scan":
      step += "perform sequential scan on table "
    else:
      step += "perform {} on table ".format(node.node_type.lower())

    step += node.read_qep_output_name()

    if not node.table_filter:
      increment = False

  elif node.node_type == "Unique":
    # combine unique and sort
    if "Sort" in node.children[0].node_type:
      node.children[0].write_qep_output_name(
        node.children[0].children[0].read_qep_output_name())
      step = "sort {} ".format(node.children[0].read_qep_output_name())
      if node.children[0].sort_key:
        step += "with attribute {} and ".format(extract_qep_conditions("Sort Key", node.children[0].sort_key, table_subquery_name_pair))
      else:
        step += " and "

    step += "perform unique on table {} ".format(node.children[0].read_qep_output_name())

  elif node.node_type == "Aggregate":
    for child in node.children:
      # combine aggregate and sort
      if "Sort" in child.node_type:
        child.write_qep_output_name(child.children[0].read_qep_output_name())
        step = "sort {} and ".format(child.read_qep_output_name())
      # combine aggregate and scan
      if "Scan" in child.node_type:
        if child.node_type == "Seq Scan":
          step = "perform sequential scan on {} and ".format(child.read_qep_output_name())
        else:
          step = "perform {} on {} and ".format(child.node_type.lower(), child.read_qep_output_name())

    step += "perform aggregate on table {}".format(node.children[0].read_qep_output_name())

    if len(node.children) == 2:
      step += " and table {} ".format(node.children[1].read_qep_output_name())

  elif node.node_type == "Sort":
    step+= "perform sort on table {} with {}".format(node.children[0].read_qep_output_name(), extract_qep_conditions("Sort Key", node.sort_key, table_subquery_name_pair))

  elif node.node_type == "Limit":
    step += "limit the result from table {} to {} record(s)".format(node.children[0].read_qep_output_name(), node.plan_rows)
  
  else:
    step += "perform {} on ".format(node.node_type.lower())

    if len(node.children) > 1:
      for i, child in enumerate(node.children):
        if i < len(node.children) - 1:
          step += (" table {},".format(child.read_qep_output_name()))
        else:
          step += (" and table {} ".format(child.read_qep_output_name()))
    else:
      step+= " table {}".format(node.children[0].read_qep_output_name())
  
  if node.group_key:
    step += " with grouping on attribute {}".format(extract_qep_conditions("Group Key", node.group_key, table_subquery_name_pair))
  if node.table_filter:
    step += " and filtering on {}".format(extract_qep_conditions("Table Filter", node.table_filter, table_subquery_name_pair))
  if node.join_filter:
    step += " while filtering on {}".format(extract_qep_conditions("Join Filter", node.join_filter, table_subquery_name_pair))

  if increment:
    node.write_qep_output_name("T" + str(cur_table_name))
    step += " to get intermediate table " + node.read_qep_output_name()
    cur_table_name += 1
  if node.subplan_name:
    table_subquery_name_pair[node.subplan_name] = node.read_qep_output_name()

  node.update_desc(step)
  step = "\nStep {}, {}.".format(cur_step, step)
  node.set_step(cur_step)
  cur_step += 1

  steps.append(step)


def extract_qep_conditions(op_name, conditions, table_subquery_name_pair):
  """
  Args:
      op_name (string): Name of the operation - Sort, Join, etc.
      conditions (dynamic): Attribute to filter upon. This can be a key or a operation condition of a node
      table_subquery_name_pair ([type]): [description]
  Returns:
      string : explanation for the condition to be met
  """

  if isinstance(conditions, str):
    if "::" in conditions:
      return conditions.replace("::", " ")[1:-1]
    return conditions[1:-1]
  cond = ""
  for i in range(len(conditions)):
    cond = cond + conditions[i]
    if (not (i == len(conditions) - 1)):
      cond = cond + "and"
  return cond


def compare_children_nodes(nodeA, nodeB, difference, reasons):
  """
  This function recursively traveses both the plan trees and compares the corresponding nodes 
  Args:
      nodeA (Node): input node to be compared (AQP)
      nodeB (Node): target node to be compared against (QEP)
      difference (string): structural difference between two nodes
      reasons (string): explanation for difference between the nodes
  """
  print(f"comparing children nodes AQP.{nodeA.node_type} and QEP.{nodeB.node_type}")
  global diff_idx
  childrenA = nodeA.children
  childrenB = nodeB.children
  children_no_A = len(childrenA)
  children_no_B = len(childrenB)

  # If both node_types are the same and they have the same number of children (!= 0), compare each corr pair of children
  if nodeA.node_type == nodeB.node_type and children_no_A == children_no_B:
    if children_no_A != 0:
      for i in range(len(childrenA)):
        compare_children_nodes(childrenA[i], childrenB[i], difference, reasons)

  else:
    # If AQP node_type == "Hash" or "Sort"
    if nodeA.node_type == 'Hash' or nodeA.node_type == 'Sort':
      text = f"Difference {diff_idx}: {nodeA.children[0].description} has been transformed to {nodeB.description}"

      text = modify_text(text)
      difference.append(text)
      reason = generate_node_diff_reason(nodeA.children[0], nodeB, diff_idx)
      reasons.append(reason)
      diff_idx += 1

    # If QEP node_type == "Hash" or "Sort"
    elif nodeB.node_type == 'Hash' or nodeB.node_type == 'Sort':
      text = f"Difference {diff_idx}: {nodeA.description} has been transformed to {nodeB.children[0].description}"

      text = modify_text(text)
      difference.append(text)
      reason = generate_node_diff_reason(nodeA, nodeB.children[0], diff_idx)
      reasons.append(reason)
      diff_idx += 1

    elif 'Gather' in nodeA.node_type:
      compare_children_nodes(childrenA[0], nodeB, difference, reasons)

    elif 'Gather' in nodeB.node_type:
      compare_children_nodes(nodeA, childrenB[0], difference, reasons)
      
    else:
      text = f"Difference {diff_idx}: {nodeA.description} has been transformed to {nodeB.description}"
      text = modify_text(text)
      difference.append(text)
      reason = generate_node_diff_reason(nodeA, nodeB, diff_idx)
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


def generate_node_diff_reason(node_a, node_b, diff_idx):
  """
  This function is used to generate the reasons for the difference in the nodes of the two QEPs being compared. The function 
  compare_children_nodes() calls this function when it is comparing the two children nodes.
  Args:
      node_a (Node): input node (AQP)
      node_b (Node): target node (QEP)
      diff_idx (int): index of the difference for which the reason is being generated
  Returns:
      string : Reason for difference between input and target nodes
  """
  text = ""
  if node_a.node_type == "Index Scan" and node_b.node_type == "Seq Scan":
    text = f"Difference {diff_idx} Reasoning: "
    text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to Sequential Scan in QEP on relation {node_b.relation_name}. This can be attributed to "
    # check conditions for transformation from end condition. Here seq scan doesn't use index
    if node_b.index_name is None:
      text += f"AQP uses the index attribute {node_a.index_name} for selection, which is not used by QEP"
    if int(node_a.actual_rows) < int(node_b.actual_rows):
      text += f"and due to this, the actual row count returned increases from {node_a.actual_rows} to {node_b.actual_rows}. "

    if node_a.index_condition != node_b.table_filter and int(node_a.actual_rows) < int(node_b.actual_rows):
      text += "This behavior is generally consistent with the change in the selection predicates from {} to {}.".format(node_a.index_condition if node_a.index_condition is not None else "None", node_b.table_filter if node_b.table_filter is not None else "None")
      
  elif node_b.node_type == "Index Scan" and node_a.node_type == "Seq Scan":
    text = f"Difference {diff_idx} Reasoning: "
    text += f"Sequential Scan in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to "
    if node_a.index_name is None:  
      text += f"QEP uses the index attribute {node_b.index_name} for selection, which is not used by AQP."
    elif node_a.index_name is not None:
      text += f"Both AQP and QEP use their index attributes for selection, which are {node_a.index_name} and {node_b.index_name} respectively."
    if int(node_a.actual_rows) > int(node_b.actual_rows):
      text += f"Due to this, the actual row count returned decreases from {node_a.actual_rows} to {node_b.actual_rows}. "
    if node_a.table_filter != node_b.index_condition and int(node_a.actual_rows) > int(node_b.actual_rows):
      text += "This behavior is generally consistent with the change in the selection predicates from {} to {}.".format(node_a.table_filter if node_a.table_filter is not None else "None", node_b.index_condition if node_b.index_condition is not None else "None")

  elif node_a.node_type == "Index Scan" and "Bitmap" in node_b.node_type and "Scan" in node_b.node_type:
    text = f"Difference {diff_idx} Reasoning: "
    text += f"Index Scan in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to the fact that more than a handful of data must be read, so PostgreSQL decided to use {node_b.node_type} which is more efficient in reading a larger number of records than Index Scan."

  elif node_b.node_type == "Index Scan" and "Bitmap" in node_a.node_type and "Scan" in node_a.node_type:
    text = f"Difference {diff_idx} Reasoning: "
    text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to Index Scan in QEP on relation {node_b.relation_name}. This can be attributed to the fact that only a handful of data must be read, so PostgreSQL decided to use Index Scan which is more efficient in reading a small number of records."

  elif node_a.node_type and node_b.node_type in ['Merge Join', "Hash Join", "Nested Loop"]:
    text = f"Difference {diff_idx} Reasoning: "
    if node_a.node_type == "Nested Loop" and node_b.node_type == "Merge Join":
      text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to "
      if int(node_a.actual_rows) < int(node_b.actual_rows):
        text += f"the actual row count returned increases from {node_a.actual_rows} to {node_b.actual_rows}. "
      if "=" in node_b.node_type:
        text += "The join condition is performed with an equality operator. "
      text += "Both sides of the Join operator in QEP can be sorted on the join condition efficiently. "

    if node_a.node_type == "Nested Loop" and node_b.node_type == "Hash Join":
      text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to "
      if int(node_a.actual_rows) < int(node_b.actual_rows):
        text += f"the actual row count returned increases from {node_a.actual_rows} to {node_b.actual_rows}. "
      if "=" in node_b.node_type:
        text += "The join condition is performed with an equality operator. "

    if node_a.node_type == "Merge Join" and node_b.node_type == "Nested Loop":
      text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to "
      if int(node_a.actual_rows) > int(node_b.actual_rows):
        text += f"the actual row count returned decreases from {node_a.actual_rows} to {node_b.actual_rows}. "
      elif int(node_a.actual_rows) < int(node_b.actual_rows):
        text += f"the actual row count returned increases from {node_a.actual_rows} to {node_b.actual_rows}. "
        text += f"{node_b.node_type} joins are used in the scenario where the join conditions are not performed with the equality operator. "
        
    if node_a.node_type == "Merge Join" and node_b.node_type == "Hash Join":
      text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to "

      if int(node_a.actual_rows) < int(node_b.actual_rows):
        text += f"the actual row count returned increases from {node_a.actual_rows} to {node_b.actual_rows}. "
      if int(node_a.actual_rows) > int(node_b.actual_rows):
        text += f"the actual row count returned decreases from {node_a.actual_rows} to {node_b.actual_rows}. "
      text += "Both sides of the Join operator in Plan 2 can be sorted on the join condition efficiently. "

    if node_a.node_type == "Hash Join" and node_b.node_type == "Nested Loop":
      text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to "
      if int(node_a.actual_rows) > int(node_b.actual_rows):
        text += f"the actual row count returned decreases from {node_a.actual_rows} to {node_b.actual_rows}. "
      elif int(node_a.actual_rows) < int(node_b.actual_rows):
        text += f"the actual row count returned increases from {node_a.actual_rows} to {node_b.actual_rows}. "
        text += f"{node_b.node_type} joins are used in the scenario where the join conditions are not performed with the equality operator. "

    if node_a.node_type == "Hash Join" and node_b.node_type == "Merge Join":
      text += f"{node_a.node_type} in AQP on relation {node_a.relation_name} has now transformed to {node_b.node_type} in QEP on relation {node_b.relation_name}. This can be attributed to "
      if int(node_a.actual_rows) < int(node_b.actual_rows):
        text += f"the actual row count returned increases from {node_a.actual_rows} to {node_b.actual_rows}. "
      if int(node_a.actual_rows) > int(node_b.actual_rows):
        text += f"the actual row count returned decreases from {node_a.actual_rows} to {node_b.actual_rows}. "
      text += "Both sides of the Join operator in Plan 2 can be sorted on the join condition efficiently. "

  return text


def reset_vars():
  """
  This function resets all the global variables to its default values so that a new QEP can be used as input
  """
  global steps, cur_step, cur_table_name, table_subquery_name_pair
  steps = []
  cur_step = 1
  cur_table_name = 1
  table_subquery_name_pair = {}










query = 'select N.n_name, R.r_name from nation N, region R where N.n_regionkey=1 and N.n_regionkey=R.r_regionkey'
qp = preprocessing.QueryPlans(query)
qep = qp.generateQEP()



json_qep = json.loads(json.dumps(qep[0][0]))
n1, nodetypes = qp.extract_qp_data(json_qep)
# print(n1.children[0].node_type)
# print('\n')
# print(nodetypes)

aqps = qp.generateAQPs(nodetypes)
# print(aqps[0])
# print('\n')
# print(aqps[1])
# print('\n')
# print(aqps[2])
# TODO if AQP and QEP are the same, then include the reason of how the specific node_type that we tried to exclude must be used so the QEP must use it
# TODO refactor code somemore
# TODO test out more queries

json_aqp = json.loads(json.dumps(aqps[0][0][0]))

root_node_qep, _ = qp.extract_qp_data(json_qep)
root_node_aqp, _ = qp.extract_qp_data(json_aqp)

print("QEP")
print(json_qep)
print("AQP")
print(json_aqp)

print(compare_two_plans(root_node_qep, root_node_aqp))
# print(json_qep[0]['Plan'])