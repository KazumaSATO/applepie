import sys
import glob

# In [3]: for filename in glob('./logs/**/*.cbor', recursive=True):
#    ...:     print(filename)


def _find_files(pattern):
    """"""
    return sorted([filename for filename in glob(pattern, recursive=True)])


def main():
    pattern = sys.argv[1]
    cbors = _find_files(pattern)
    
