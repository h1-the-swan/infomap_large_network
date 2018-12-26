
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


test_fname = '/home/jporteno/data/MAG_academicgraphdls_20180329/PaperReferences_split/PaperReferences_academicgraphdls_20180329_split_1.tsv'


# In[6]:


def load_refs(fname):
    refs = pd.read_table(fname, header=None, names=['Paper_ID', 'Paper_reference_ID'])
    return refs


# In[7]:


start = timer()
refs = load_refs(test_fname)
print(format_timespan(timer()-start))


# In[8]:


refs_fname = '/home/jporteno/data/MAG_academicgraphdls_20180329/PaperReferences_academicgraphdls_20180329.txt'


# In[9]:


start = timer()
refs = load_refs(refs_fname)
print(format_timespan(timer()-start))


# In[10]:


len(refs)


# In[11]:


# start = timer()
# relaxmap_clusters = db.read_sql("select Paper_ID, toplevel_cl from twolevel_cluster_relaxmap;")
# print(format_timespan(timer()-start))


# In[12]:


from h1theswan_utils.treefiles import Treefile
t = Treefile('/home/jporteno/data/MAG_academicgraphdls_20180329/PaperReferences_academicgraphdls_20180329.tree')
start = timer()
t.parse()
t.load_df()
t.get_top_cluster_counts()
print(format_timespan(timer()-start))


# In[13]:


start = timer()
cl_map = t.df.set_index('name').top_cluster
print(format_timespan(timer()-start))


# In[14]:


start = timer()
refs['cl_out'] = refs.Paper_ID.astype(str).map(cl_map)
print(format_timespan(timer()-start))


# In[15]:


start = timer()
refs['cl_in'] = refs.Paper_reference_ID.astype(str).map(cl_map)
print(format_timespan(timer()-start))


# In[20]:


start = timer()
refs_within_cl = refs[refs.cl_out==refs.cl_in]
print(format_timespan(timer()-start))


# In[21]:


start=timer()
refs_within_cl.to_csv('data/PaperReferences_academicgraphdls_20180329_withinCluster.tsv', sep='\t', index=False)
print(format_timespan(timer()-start))


# In[22]:


len(refs_within_cl)


# In[23]:


from h1theswan_utils.network_data import PajekFactory


# In[ ]:


start = timer()
pjk = PajekFactory()
for _, row in refs_within_cl.iterrows():
    pjk.add_edge(row.Paper_ID, row.Paper_reference_ID)
print(format_timespan(timer()-start))


# In[ ]:


start = timer()
with open('data/PaperReferences_academicgraphdls_20180329_withinCluster.net', 'w') as outf:
    pjk.write(outf, vertices_label='Vertices', edges_label='Arcs')
print(format_timespan(timer()-start))


# In[ ]:


infomap_path = '/home/jporteno/code/infomap/Infomap'
cmd = [infomap_path, 'data/PaperReferences_academicgraphdls_20180329_withinCluster.net', './data', '-t', '-vvv', '--seed 999']


# In[ ]:


import subprocess


# In[ ]:


with open('data/PaperReferences_academicgraphdls_20180329_withinCluster_infomap.log', 'w') as logf:
    process = subprocess.run(cmd, stdout=logf, stderr=subprocess.STDOUT)


# In[ ]:


from h1theswan_utils.network_data import PajekFactory


# In[ ]:


start = timer()
pjk = PajekFactory()
for df in cluster_edgelists.values():
    for _, row in df.iterrows():
        pjk.add_edge(row.Paper_ID, row.Paper_reference_ID)
print(format_timespan(timer()-start))


# In[ ]:


with open('tmp100.net' ,'w') as outf:
    pjk.write(outf, vertices_label='Vertices', edges_label='Arcs')


# In[ ]:


from h1theswan_utils.treefiles import Treefile


# In[ ]:


t = Treefile('tmp100.tree')
t.parse()


# In[ ]:


t.load_df()
t.get_top_cluster_counts()
t.df


# In[ ]:


cl_map = t.df.set_index('name').top_cluster.astype('int')


# In[ ]:


for df in cluster_edgelists.values():
    df['infomap_top_cl'] = df.Paper_ID.astype(str).map(cl_map)


# In[ ]:


for k, v in cluster_edgelists.items():
    print(k)
    print(v.infomap_top_cl.value_counts())
    print()


# In[ ]:


start = timer()
df = cl_paperrefs_join_query(14975)
print(format_timespan(timer()-start))


# In[ ]:


len(set.union(set(df.Paper_ID), set(df.Paper_reference_ID)))

