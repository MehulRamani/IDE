
# coding: utf-8

# In[26]:

import pandas as pd
import re
import numpy as np
from datetime import datetime
import pytz


# In[27]:

def parse_datetime(x):
    '''
    Parses datetime with timezone formatted as:
        `[day/month/year:hour:minute:second zone]`

    Example:
        `>>> parse_datetime('13/Nov/2015:11:45:42 +0000')`
        `datetime.datetime(2015, 11, 3, 11, 45, 4, tzinfo=<UTC>)`

    Due to problems parsing the timezone (`%z`) with `datetime.strptime`, the 
    timezone will be obtained using the `pytz` library.
    '''    
    dt = datetime.strptime(x[1:-7], '%d/%b/%Y:%H:%M:%S')
    dt_tz = int(x[-6:-3])*60+int(x[-3:-1])    
    return dt.replace(tzinfo=pytz.FixedOffset(dt_tz))


# In[29]:

import re
import pandas as pd

data = pd.read_csv(
    '../log_input/log.txt', 
    sep=r'\s(?=(?:[^"]*"[^"]*")*[^"]*$)(?![^\[]*\])', 
    engine='python', 
    header=None,
    na_values='-',
    usecols=[0, 3, 4, 5, 6],
    names=['ip', 'time', 'request', 'status', 'size'],
    converters={'time': parse_datetime}
                    )


# In[30]:

data['mins'] = data['time'].map(lambda x : x.minute)
data['hrs'] = data['time'].map(lambda x : x.hour)
data['sc'] = data['time'].map(lambda x: x.second)
data['path']= data['request'].map(lambda x: str(x)[4:-1])


# List in descending order the top 10 most active hosts/IP addresses that have accessed the site.
# 
# Write to a file, named hosts.txt, the 10 most active hosts/IP addresses in descending order and how many times they have accessed any part of the site

# In[31]:

data['ip'].value_counts()[:10].to_csv("../log_output/hosts.txt", header = False, mode = 'w')


# Identify the 10 resources that consume the most bandwidth on the site
# Identify the top 10 resources on the site that consume the most bandwidth. Bandwidth consumption can be extrapolated from bytes sent over the network and the frequency by which they were accessed.

# In[33]:

data.head()


# In[34]:

data.sort_values(by='size', ascending=False)[:10].to_csv("../log_output/resources.txt", columns = ["path"], header = False, index= False, mode = 'w')


# List in descending order the site’s 10 busiest (i.e. most frequently visited) 60-minute period.
# 
# Write to a file named hours.txt, the start of each 60-minute window followed by the number of times the site was accessed during that time period. The file should contain at most 10 lines with each line containing the start of each 60-minute window, followed by a comma and then the number of times the site was accessed during those 60 minutes. The 10 lines should be listed in descending order with the busiest 60-minute window shown first.
# 
# 

# In[35]:

grp_mins = data.groupby(['hrs','time']).size().reset_index(name='count')


# In[36]:

grp_mins[:10].to_csv("../log_output/hours.txt", columns = ["time","count"], header = False, index= False, mode = 'w')


# In[ ]:



