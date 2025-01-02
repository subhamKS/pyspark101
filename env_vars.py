import os

os.environ['env'] = 'dev'
os.environ['header'] = 'True'
os.environ['inferschema'] = 'True'

header = os.environ['header']
env = os.environ['env']
inferschema = os.environ['inferschema']
