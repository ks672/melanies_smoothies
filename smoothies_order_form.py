# Import python packages
import streamlit as st
#Need to manually connect to snowflake in the web version of streamlit
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

import pandas as pd

#api call for fruit data in a json format
import requests

# Write directly to the app
st.title(f"Customise your smoothie laddie! :cup_with_straw:")
st.write(
  """Choose the ingredients you want in your smoothie
  """
)


name_on_order = st.text_input("Name on Smoothie", "")

#if name_on_order:
st.write("The name on your smoothie will be:", name_on_order)

#Need to manually connect to snowflake in the web version of streamlit
#session = get_active_session()
cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.fruit_options").select(col('FRUIT_NAME'),col('SEARCH_ON'))
#st.dataframe(data=my_dataframe, use_container_width=True)
#st.stop()

#Convert the Snowpark Dataframe to a Pandas Dataframe so we can use the LOC function
pd_df=my_dataframe.to_pandas()
st.dataframe(pd_df)
st.stop()

ingredients_list = st.multiselect(
    'Choose up to 5 ingredients:'
    ,my_dataframe
    ,max_selections= 5
)

if ingredients_list:
        #st.write(ingredients_list)
        #st.text(ingredients_list)

        ingredients_string = ''

        for x in ingredients_list:
            ingredients_string += x + ' '

            search_on = pd_df.loc[pd_df['FRUIT_NAME'] == x, 'SEARCH_ON'].iloc[0]
            st.write('The search value for ', x,' is ', search_on, '.')

            st.subheader(x + 'Nutrition Information')
            smoothiefroot_response = requests.get("https://my.smoothiefroot.com/api/fruit/" + x)
            #st.text(smoothiefroot_response.json())
            st_df = st.dataframe(data=smoothiefroot_response.json(), use_container_width=True)

        st.write(ingredients_string)

        my_insert_stmt = """ insert into smoothies.public.orders(ingredients, name_on_order)
            values ('""" + ingredients_string + """','""" + name_on_order + """')"""

        #st.write(my_insert_stmt)
        time_to_insert = st.button('Sumbit Order')
    
        if time_to_insert:
            session.sql(my_insert_stmt).collect()
            
            st.success('Your Smoothie is ordered!', icon="✅")



#option = st.selectbox(
#    "Choose your fruit?",
#    ('Banana', 'Strawberries', 'Berries'),
#)

#st.write("You selected:", option)



#st.write("Underlying data:")
#st.dataframe(data=my_dataframe, use_container_width=True)
