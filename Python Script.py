#!/usr/bin/env python
# coding: utf-8

# # Bigdata Hadoop MapReduce Alpha Count Python visualization file
# 
# ### Abhishek Dubey D20123718
# 
# ##### Python Script
# 

# In[1]:


import pandas as pd
import matplotlib.pyplot as plt


# In[2]:


df = pd.read_csv(r"C:\Users\abhis\Desktop\hadoop book\_user_soc_englishcount_part-r-00000", sep='\t', names=["language", "alphabet", "average"])


# In[3]:


df=df.set_index('alphabet')


# In[4]:


df["average"].plot(kind="bar", figsize=(12,6))


# In[5]:


df_french = pd.read_csv(r"C:\Users\abhis\Desktop\hadoop book\_user_soc_frenchcount_part-r-00000", sep='\t', names=["language", "alphabet", "average"])


# In[6]:


df_french = df_french.set_index('alphabet')


# In[7]:


df_french["average"].plot(kind="bar", figsize=(12,6))


# In[8]:


df_spanish = pd.read_csv(r"C:\Users\abhis\Desktop\hadoop book\_user_soc_spanishcount_part-r-00000", sep='\t', names=["language", "alphabet", "average"])


# In[9]:


df_spanish = df_spanish.set_index('alphabet')


# In[10]:


df_spanish["average"].plot(kind="bar", figsize=(12,6))


# In[11]:


df=df.rename(columns={"average": "English_Alpha_Average"})
df_french=df_french.rename(columns={"average": "French_Alpha_Average"})
df_spanish=df_spanish.rename(columns={"average": "Spanish_Alpha_Average"})


# In[12]:


Final_df=pd.concat([df,df_french, df_spanish],axis=1)


# In[13]:


Final_df[["English_Alpha_Average","French_Alpha_Average","Spanish_Alpha_Average"]].plot(kind="bar", figsize=(17,7))


# In[ ]:




