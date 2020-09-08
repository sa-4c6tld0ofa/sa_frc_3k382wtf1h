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

def score_increment(what):
    """ 
    Return the increment number if the trade is a win or a loss.
    what = 'win', 'loss'
    """
    ret = ''
    win_incr = 0.01
    loss_incr = -0.02
    if what == 'win':
        ret = win_incr
    else:
        ret = loss_incr
    return ret

def debug(txt):
    """ If enable_debug is True, then print to console """
    enable_debug = False
    try:
        if enable_debug: print(txt)
    except:
        pass
