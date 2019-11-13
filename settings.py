""" Settings of the programme """
import os

class SmartAlphaPath:
    rdir = os.path.dirname(os.path.realpath(__file__))
    pdir = os.path.abspath(os.path.join(rdir, os.pardir))

    def get_path_pwd(self):
        """ Get path to access information """
        return self.pdir+ "\\sa_pwd"

    def get_path_src(self):
        """ Get access to data files and data source """
        return self.pdir+"\\sa_data_collection\\src\\"

    def get_path_core(self):
        """ Get access to main modules for data collection """
        return self.pdir + "\\sa_data_collection\\core"

def get_portf_suffix():
    """ Return strategy portfolio suffix """
    return "PRF:"

def debug(txt):
    """ If enable_debug is True, then print to console """
    enable_debug = False
    try:
        if enable_debug: print(txt)
    except:
        pass
