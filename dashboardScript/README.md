# PhonePe Pulse - Data

**requirement.txt**

This file has all required libraries needed to be installed along with their version using Python Version = 3.11.1

#### Step 1:
use dropcreatetables.sql file to create required tables.
1. agg_tdata = it has aggregated transaction data
2. agg_udata = it has aggregated user data
3. districts = it has all districts with respect to its states for both transaction and users.
    a. The columns entityName,metric_count and metric_amount represent the transaction fields and this can be filtered using transaction_or_users = 'transaction'
    b. The columns name, registeredUsers represent the users fields and this can be filtered using transaction_or_users = 'users'
4. pincodes = it has all pincodes with respect to its states for both transaction and users.
    a. The columns entityName,metric_count and metric_amount represent the transaction fields and this can be filtered using transaction_or_users = 'transaction'
    b. The columns name, registeredUsers represent the users fields and this can be filtered using transaction_or_users = 'users'    
5. merged_geolocation_data = it has all four tables combined with a field transaction_or_users which can be used to filter out transaction and users. This table has states and districts geolocations.


#### Step 2:
After creating the required tables, run convert_push_data_git_to_postgres.py to push records into the first 4 tables in PostgresSQL. These records are extracted from Dataset Link: https://github.com/PhonePe/pulse and are modified for required needs.

#### Step 3:
Once the data is pushed, create the 2 views for each transaction and users data using createorreplaceview.sql file.

#### Step 4:
Now retrieve these views and push it into 5th table as mentioned above using retrieve_and_transform.py

#### Step 5:
run the main.py using streamlit command to visualise the map along with its details.
The map has following filters:
Dropdowns:
    1. Transaction or Users
    2. State
    3. Year
    4. Quarter
    5. District
    6. Brand
    7. transaction category
Checkboxes:
    For users, based upon total aggregated registered users:
        1. Top 10 Brands
        2. Top 10 State
        3. Top 10 District
    For transaction, based upon total transaction amount users:
        1. Top 10 transactions
        2. Top 10 State
        3. Top 10 District

**Notes to remember:**
*1. When the transaction option is selected, the transaction category dropdown is visible and when users is selected, the brand option is visible.*
*2. When a state is selected, then district geolocations are visible*
*3. For checkboxes, you will get the results of the selected filter(s)*