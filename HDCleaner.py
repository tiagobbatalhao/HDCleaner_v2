import os
import pandas
import hashlib
import logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s %(message)s'
)

def get_size(path):
    try:
        return os.path.getsize(path)
    except FileNotFoundError:
        return None

def list_files(path, with_size=True):
    """
    List files in a directory
    """
    files = []
    for (dirpath, dirnames, filenames) in os.walk(path):
        files += [os.path.join(dirpath, fl) for fl in filenames]
    if with_size:
        files = [(x, get_size(x)) for x in files]
    return files

def remove_common_path(files):
    """
    From a list of files, return a new list
    with the common prefix removed
    """
    common_path = os.path.commonpath(files)
    remove = len(common_path)
    removed = [x[remove:].lstrip('/') for x in files]
    return common_path, removed

def split_path(path):
    """
    Split a path in folders
    https://www.oreilly.com/library/view/python-cookbook/0596001673/ch04s16.html
    """
    allparts = []
    while 1:
        parts = os.path.split(path)
        if parts[0] == path:  # sentinel for absolute paths
            allparts.insert(0, parts[0])
            break
        elif parts[1] == path: # sentinel for relative paths
            allparts.insert(0, parts[1])
            break
        else:
            path = parts[0]
            allparts.insert(0, parts[1])
    return allparts

def file_visibility(path):
    """
    Find the type of file
    * Visible
    * Git
    * DS_Store
    * Other hidden files
    """
    parts = split_path(path)
    for part in parts[::-1]:
        if part.startswith('.DS_Store'):
            return 'DS_Store'
        elif part.startswith('.git'):
            return 'git'
        elif part.startswith('.'):
            return 'hidden'
    return 'visible'

def file_levels(path, as_dict=False):
    """
    Return a sequence of the file-tree levels
    """
    parts = split_path(path)
    levels = [os.path.join(*parts[:i]) for i in range(1,1+len(parts))]
    if as_dict:
        return {i: x for i, x in enumerate(levels)}
    else:
        return levels

def main_pandas(path):
    """
    Return a Pandas Dataframe
    with some information about files
    """
    df = pandas.DataFrame()

    files = list_files(path, with_size=True)
    df['filepath'] = [x[0] for x in files]
    df['size'] = [x[1] for x in files]
    df = df.drop_duplicates('filepath')
    df['type'] = df['filepath'].apply(file_visibility)
    df['extension'] = df['filepath'].apply(lambda x: os.path.splitext(x)[1])
    levels = df['filepath'].apply(lambda x: file_levels(x, as_dict=False))
    level_len = levels.apply(len)
    for i in range(max(level_len)):
        flter = level_len > i
        df.loc[flter, f'level_{i:02d}'] = levels.loc[flter].apply(lambda x: x[i])

    return df

def hash_MD5(path):
    """
    MD5 hash of a file
    """
    hasher = hashlib.md5()
    logging.info(f'Starting to hash {path}')
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hasher.update(chunk)
    except (FileNotFoundError, OSError) as error:
        print(error)
        return None
    logging.info(f'Finished hashing {path}')
    return hasher.hexdigest()

def find_issues_pandas(dataframe):
    """
    Return sets of files that may be the same
    """
    df = dataframe.copy()
    df = df[df['type']=='visible']

    nunique = df\
    .groupby('size', as_index=False)\
    .agg({'filepath': 'nunique'})\
    .rename(columns={'filepath': 'nunique_size'})
    df = df.merge(nunique, how='left', on='size')

    flter = df['nunique_size'] > 1
    df = df[flter]
    df['hashMD5'] = \
        df['filepath'].apply(
            lambda x: hash_MD5(x)
        )

    unique = df\
    .groupby(['size', 'hashMD5'], as_index=False)\
    .agg({'filepath': 'unique'})
    unique = unique\
        [unique['filepath'].apply(len)>1]\
        .sort_values(by='size', ascending=False)\
        .drop('hashMD5', axis=1)\
        .reset_index(drop=True)
    return unique

def find_issues_master_client(
        dataframe_master,
        dataframe_client,
        min_size=0,
        only_visible=True,
    ):
    """
    Delete files in client if they are present in master
    """
    # Check if there are common files in master and client
    common_files = \
        set(dataframe_master['filepath'].unique())\
        .intersection(set(dataframe_client['filepath'].unique()))
    if len(common_files):
        raise Exception('Master and client must not contain the same files')

    # Filter too-small files
    df_master = dataframe_master[dataframe_master['size']>=min_size]
    df_client = dataframe_client[dataframe_client['size']>=min_size]

    # Filter only files that are Visible
    if only_visible:
        df_master = df_master[df_master['type']=='visible']
        df_client = df_client[df_client['type']=='visible']

    # Filter only files that have a size in common with the other
    common_sizes = \
        set(df_master['size'].unique())\
        .intersection(set(df_client['size'].unique()))
    df_master = df_master[df_master['size'].isin(common_sizes)]
    df_client = df_client[df_client['size'].isin(common_sizes)]

    # Hashing all the files
    df_master['hashMD5'] = df_master['filepath'].apply(lambda x: hash_MD5(x))
    df_client['hashMD5'] = df_client['filepath'].apply(lambda x: hash_MD5(x))

    # Filter out files that could not be hashed
    df_master = df_master[~df_master['hashMD5'].isnull()]
    df_client = df_client[~df_client['hashMD5'].isnull()]

    # Inner join for same_files
    same_files = \
        df_client\
        .merge(
            df_master[['size', 'hashMD5', 'filepath']],
            how='inner',
            on=['size', 'hashMD5'],
            suffixes=['', '_master'],
        )
    return same_files
