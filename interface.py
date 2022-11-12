"""
Contains code for the GUI
"""
import streamlit as st
import preprocessing
import annotation
import json

st.title('SQL Query Optimizer')

st.subheader('Enter your query input')

input_text = st.text_area('Enter query', height=300, label_visibility='collapsed')

flag = True

# this will put a button in the middle column
if st.button('Execute'):
    with st.spinner('Loading...'):
        st.subheader('Query Visualization')

        result = preprocessing.explainQuery(input_text,format='text')
        print("result =\n", result)
        # In case connection with database fails (Display message)
        if(result[0] == 'Connection Error'):
            st.text('Database Error. Please update database.ini and check if your query is correct syntax')
            raise Exception("Database Error. Please update database.ini and check if your query is correct syntax")

        # Splitting string on new line
        list_nodes = result.split("\n")

        # Deciding which nodes will go on the left side of the graph (Assuming that it would always be a left deep tree)
        left_node = [True]  # First node Will come on left side of the graph

        list_nodes, left_node = preprocessing.stringProcess(list_nodes, left_node) 

        qp = preprocessing.QueryPlans(input_text)
        qep = qp.generateQEP()
        json_qep = json.loads(json.dumps(qep[0][0]))
        n1, nodetypes = qp.extract_qp_data(json_qep)

        aqps = qp.generateAQPs(nodetypes)

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
            diff_str_flag = ""

            if diff_str == "":
                diff_str = f"QEP and this generated AQP ({i+1}) are the same as the node type that we tried to exclude ({nt}) must be used (no other alternative available)"
            else:
                flag = False
                diff_str_flag = diff_str
            
            print(diff_str+"\n")

        if flag:
            st.text(diff_str)
        else:
            st.text(diff_str_flag)

        plot0 = annotation.show_graph(list_nodes, left_node)
        st.pyplot(plot0)

