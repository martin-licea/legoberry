import streamlit as st
from transformations_ui import show_fields

def upload_file():
    col1, col2, col3 = st.columns([1,1,1])
    uploaded_file = col1.file_uploader("Choose your Data file")
    if uploaded_file is not None:
        file_details = {"FileName":uploaded_file.name,"FileType":uploaded_file.type,"FileSize":uploaded_file.size}
        col1.write(file_details)
        return uploaded_file
    return None

def main_page(profile):
    st.write("Input Data Files")
    uploaded_file = upload_file()
    
    show_fields(profile)


