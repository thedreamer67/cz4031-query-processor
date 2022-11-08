"""
Contains code for the GUI
"""
import streamlit as st
import preprocessing
from annotation import show_graph

st.title('SQL Query Optimizer')

st.subheader('Enter your query input')

input_text = st.text_area('Enter query', height=300, label_visibility='collapsed')

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

        plot0 = show_graph(list_nodes, left_node)
        st.pyplot(plot0)

