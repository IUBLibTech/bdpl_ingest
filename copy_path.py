import pyperclip
import sys

path = sys.argv[1].replace('\\', '/')

pyperclip.copy(path)