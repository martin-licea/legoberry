from typing import List
import streamlit as st
import yaml
from pathlib import Path
from sections import main_page
import os 

st.set_page_config(page_title="LegoBerry", page_icon="🧱", layout="wide")
st.title("LegoBerry")

def get_preloaded_profiles() -> List[str]:
    current_folder = os.path.dirname(os.path.realpath(__file__))
    profiles_dir = Path(current_folder) / "profiles"
    profiles = list(profiles_dir.glob("*.yml"))
    return profiles

def get_profile_name(profile):
    file_name = Path(profile).stem
    file_name = file_name.replace("_", " ")
    file_name = file_name.title()
    return file_name

def main():
    profiles = get_preloaded_profiles()
    uploaded_file = st.sidebar.file_uploader("Import a Profile")
    st.sidebar.write("Create a New Profile")
    col1, col2 = st.sidebar.columns([2,1])
    profile_name = col1.text_input("Profile Name", key="profile_name", label_visibility='hidden').title()
    col2.write(" ")
    col2.write(" ")
    create_new_profile = col2.button("Save")
    if create_new_profile or profile_name:
        profile_path = Path(__file__).parent / "profiles" / f"{profile_name.replace(' ', '_').lower()}.yml"
        with open(profile_path, "w") as f:
            yaml.dump({}, f)
        if profile_path not in profiles:
            profiles.append(profile_path)
        else:
            st.sidebar.warning(f" A Profile with the name \"{profile_name}\" already exists. Try a different name.")
    
    st.sidebar.write("Select a Profile")
    for profile in profiles:
        profile_name = get_profile_name(profile)
        if st.sidebar.button(profile_name):
            st.write(f"Selected profile: {profile_name}")
            main_page(profile)
    
    with st.sidebar.expander("Danger Zone"):
        st.sidebar.write("Delete a Profile")
        for profile in profiles:
            profile_name = get_profile_name(profile)
            if st.sidebar.button(f"{profile_name}", type='secondary', key=f"{profile_name}_delete"):
                confirm_delete = st.text_input("Are you sure? Type 'delete' to confirm", key=f"{profile_name}_delete_confirm")
                if confirm_delete == "delete":
                    os.remove(profile)
                    st.write(f"Deleted profile: {profile_name}")
    


if __name__ == "__main__":      
    main()

