# In[1]:


def simulate_demo(tc):
    for task in [
        "create_schema",
        "create_table",
        "insert_into_table",
        "test_select_query",
    ]:
        tc.execute_operation_by_name(task)
        print("--------------------------")


def run_demo(tc):
    """
    Execute the same steps; only print final SELECT results.
    """
    for task in ["create_schema", "create_table", "insert_into_table"]:
        tc.execute_operation_by_name(task)
        tc.run_query()

    tc.execute_operation_by_name("test_select_query")
    rows = tc.run_query()
    print(f"Results: {rows}")
