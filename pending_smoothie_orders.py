# Import python packages
import streamlit as st
#Need to manually connect to snowflake in the web version of streamlit
#from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched

# Write directly to the app
st.title(f"Here are the new kitchen orders! :clipboard:")
st.write(
  """Click on the check mark to mark orders as complete
  """
)

#Need to manually connect to snowflake in the web version of streamlit
#session = get_active_session()
cnx = st.connection("snowflake")
session = cnx.session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if my_dataframe:
    editable_df = st.data_editor(my_dataframe, disabled=["INGREDIENTS", "NAME_ON_ORDER", "ORDER_UID", "ORDER_TS"], hide_index=True)
    submit = st.button('Complete orders')
    
    if submit:
#    update_stmt = """ update smoothies.public.orders where  = """' + """ 
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)

        try:
            og_dataset.merge(edited_dataset
                             , (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID'])
                             , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                            )
            st.success('Order(s) Updated!', icon="✅")

        except:
            st.write('Something went wrong')

else: 
    st.success('No pending orders', icon="👍");



st.write(
  """All completed orders :white_check_mark:
  """
)

completed_orders = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==1).collect()

if completed_orders:
    st.dataframe(data=completed_orders, use_container_width=True)

else:
    st.success('No completed orders to display')


#if
#update_stmt = """ update smoothies.public.orders where name_on_order = """' + '""" 

#st.dataframe(data=edit_dataframe, use_container_width=True)




#update_stmt = """ update smoothies.public.orders where name_on_order = """' + ''""" 


#if time_to_insert:
#            session.sql(my_insert_stmt).collect()
#            st.success('Your Smoothie is ordered!', icon="✅")


