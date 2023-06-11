import streamlit as st
from sqlmanager import SQLManager
import dataprocessor as dp

if __name__ == '__main__':
    '''# hi'''
    user_input = dp.input_filter()
    dp.print_filtered_result(SQLManager('data.db'), **user_input)
