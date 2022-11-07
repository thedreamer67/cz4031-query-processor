"""
Main file that invokes all the necessary procedures from the other 3 files - interface.py, annotation.py, preprocessing.py
"""

import annotation, preprocessing, interface
import json

# Generate QEP based on query
query = 'select N.n_name, R.r_name from nation N, region R where N.n_regionkey=1 and N.n_regionkey=R.r_regionkey'
qp = preprocessing.QueryPlans(query)
qep = qp.generateQEP()
json_qep = json.loads(json.dumps(qep[0][0]))
n1, nodetypes = qp.extract_qp_data(json_qep)
# print(n1.children[0].node_type)
# print('\n')
# print(nodetypes)

# Generate AQPS by excluding certain nodes found in QEP
aqps = qp.generateAQPs(nodetypes)

# Compare QEP and each AQP generated, and print the difference(s) if any
for i, nt in enumerate(nodetypes):
  print(f"Comparing QEP and AQP {i+1}")
  json_aqp = json.loads(json.dumps(aqps[i][0][0]))

  print("QEP")
  print(json_qep)
  print("AQP")
  print(json_aqp)


  root_node_qep, _ = qp.extract_qp_data(json_qep)
  root_node_aqp, _ = qp.extract_qp_data(json_aqp)


  diff_str = annotation.compare_two_plans(root_node_qep, root_node_aqp)
  if diff_str == "":
    diff_str = f"QEP and this generated AQP ({i+1}) are the same as the node type that we tried to exclude ({nt}) must be used (no other alternative available)"
  
  print(diff_str+"\n")


# TODO refactor code somemore
# TODO test out more queries