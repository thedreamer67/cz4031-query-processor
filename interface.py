"""
Contains code for the GUI
"""
from configparser import ConfigParser
import streamlit as st
import preprocessing
import annotation
import json
from preprocessing import config
import psycopg2


def connect_to_database():
    conn = None
    # instantiate
    config = ConfigParser()

    # parse existing file
    config.read('database.ini')

    # read values from a section
    host_st = config.get('postgresql', 'host')
    db_st = config.get('postgresql', 'database')
    user_st = config.get('postgresql', 'user')
    pw_st = config.get('postgresql', 'password')
    port_st = config.get('postgresql', 'port')

    try:
        conn = psycopg2.connect(host=host_st, port=port_st, database = db_st, user = user_st, password=pw_st)
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return False

st.title('SQL Query Optimizer')
st.subheader('Enter your query input')

input_text = st.text_area('Enter query', height=300, label_visibility='collapsed')

flag = True

with st.sidebar:
    st.title('Enter Your Credentials')
    user = st.text_input("User:", value="")
    pwd = st.text_input("Password:", value="", type="password")
    if st.button('Login'):
        # instantiate
        config = ConfigParser()
        # parse existing file
        config.read('database.ini')
        # update existing value
        config.set('postgresql', 'password', pwd)
        config.set('postgresql', 'user', user)

        # save to a file
        with open('database.ini', 'w') as configfile:
            config.write(configfile)

        check_conn = connect_to_database()
        if check_conn != False:
            st.success(' Login Success', icon="✅")

        else:
            st.error('Login Fail', icon="🚨")

# this will put a button in the middle column
if st.button('Execute'):
    with st.spinner('Loading...'):
        st.subheader('Query Visualization')

        result = preprocessing.explainQuery(input_text,format='text')
        print("result =\n", result)

        # Splitting string on new line
        list_nodes = result.split("\n")

        # Deciding which nodes will go on the left side of the graph (Assuming that it would always be a left deep tree)
        left_node = [True]  # First node Will come on left side of the graph

        list_nodes, left_node = preprocessing.stringProcess(list_nodes, left_node) 

        # qp = preprocessing.QueryPlans(input_text)
        # qep = qp.generateQEP()
        # json_qep = json.loads(json.dumps(qep[0][0]))
        # n1, nodetypes = qp.extract_qp_data(json_qep)
        #
        # aqps = qp.generateAQPs(nodetypes)
        #
        # for i, nt in enumerate(nodetypes):
        #     print(f"Comparing QEP and AQP {i+1}")
        #     json_aqp = json.loads(json.dumps(aqps[i][0][0]))
        #
        #     print("QEP")
        #     print(json_qep)
        #     print("AQP")
        #     print(json_aqp)
        #
        #
        #     root_node_qep, _ = qp.extract_qp_data(json_qep)
        #     root_node_aqp, _ = qp.extract_qp_data(json_aqp)
        #
        #
        #     diff_str = annotation.compare_two_plans(root_node_qep, root_node_aqp)
        #     diff_str_flag = ""
        #
        #     if diff_str == "":
        #         diff_str = f"QEP and this generated AQP ({i+1}) are the same as the node type that we tried to exclude ({nt}) must be used (no other alternative available)"
        #     else:
        #         flag = False
        #         diff_str_flag = diff_str
        #
        #     print(diff_str+"\n")
        #
        # if flag:
        #     st.text(diff_str)
        # else:
        #     st.text(diff_str_flag)

        plot0 = annotation.show_graph(list_nodes, left_node)
        st.pyplot(plot0)

