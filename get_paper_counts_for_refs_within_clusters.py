
# coding: utf-8

# In[1]:


import sys, os, time
from timeit import default_timer as timer
from humanfriendly import format_timespan


# In[2]:


import pandas as pd


# In[3]:


from dotenv import load_dotenv
load_dotenv('admin.env')


# In[4]:


from mysql_connect import get_db_connection
db = get_db_connection('mag_20180329')


# In[5]:


fname = 'data/PaperReferences_academicgraphdls_20180329_withinCluster.tsv'


# In[6]:


refs_within_cl = pd.read_table(fname)


# In[7]:


len(refs_within_cl)


# In[8]:


refs_head = refs_within_cl.head(10000).copy()


# In[14]:


gb = refs_head.groupby('cl_out')
refs_head['num_rows_in_cl'] = refs_head.cl_out.map(gb.apply(lambda x: len(x)))


# In[17]:


start = timer()
gb = refs_within_cl.groupby('cl_out')
print(format_timespan(timer()-start))


# In[18]:


start = timer()
refs_within_cl['num_rows_in_cl'] = refs_within_cl.cl_out.map(gb.apply(lambda x: len(x)))
print(format_timespan(timer()-start))


# In[24]:


start = timer()
refs_within_cl_subset = refs_within_cl[refs_within_cl.num_rows_in_cl>=100]
print(format_timespan(timer()-start))


# In[26]:


refs_within_cl_subset.shape


# In[27]:


from h1theswan_utils.treefiles import Treefile
t = Treefile('/home/jporteno/data/MAG_academicgraphdls_20180329/PaperReferences_academicgraphdls_20180329.tree')
start = timer()
t.parse()
t.load_df()
t.get_top_cluster_counts()
print(format_timespan(timer()-start))


# In[ ]:


start = timer()
refs_within_cl['num_papers_in_cluster'] = refs_within_cl.cl_out.astype(str).map(t.top_cluster_counts)
print(format_timespan(timer()-start))


# In[ ]:


start=timer()
refs_within_cl.to_csv('data/PaperReferences_academicgraphdls_20180329_withinCluster_withPaperCounts.tsv', sep='\t', index=False)
print(format_timespan(timer()-start))

