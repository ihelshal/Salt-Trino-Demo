# In[1]: Import Packages

from ApplicationsLayer import app
from DatabaseHandler.Connector2Trino import TrinoClient

# In[2]:

if __name__ == "__main__":
    tc = TrinoClient()
    tc.connect_to_trino()

    # Pick one:
    # app.simulate_demo(tc)  # prints the SQL only
    app.run_demo(tc)  # actually runs it and prints rows

    tc.terminate_connection()
