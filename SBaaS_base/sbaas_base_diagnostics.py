import os

class sbaas_base_memory_linux:
    '''class to calculate the memory of a process'''
    #Source: http://code.activestate.com/recipes/286222/
    def __init__(self):
        try:
            self._proc_status = '/proc/%d/status' % os.getpid()

            self._scale = {'kB': 1024.0, 'mB': 1024.0*1024.0,
                      'KB': 1024.0, 'MB': 1024.0*1024.0}
        except Exception as e:
            print(e)
            
    def _VmB(self,VmKey):
        '''Private.
        '''
        try:
            t = open(self._proc_status)
            v = t.read()
            t.close()
        except:
            return 0.0  # non-Linux?
         # get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
        i = v.index(VmKey)
        v = v[i:].split(None, 3)  # whitespace
        if len(v) < 3:
            return 0.0  # invalid format?
         # convert Vm value to bytes
        return float(v[1]) * self._scale[v[2]]

    def get_memory(self,since=0.0):
        '''Return memory usage in bytes.
        '''
        return self._VmB('VmSize:') - since

    def get_resident(self,since=0.0):
        '''Return resident memory usage in bytes.
        '''
        return self._VmB('VmRSS:') - since

    def get_stacksize(self,since=0.0):
        '''Return stack size in bytes.
        '''
        return self._VmB('VmStk:') - since

class sbaas_base_memory_windows:
    def get_memory(self):
        try:
            from wmi import WMI
            w = WMI('.')
            result = w.query("SELECT WorkingSet FROM Win32_PerfRawData_PerfProc_Process WHERE IDProcess=%d" % os.getpid())
            return int(result[0].WorkingSet)
        except ImportError as e:
            print(e);