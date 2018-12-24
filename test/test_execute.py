#!/usr/bin/env python3
#
# Copyright (c) Bo Peng and the University of Texas MD Anderson Cancer Center
# Distributed under the terms of the 3-clause BSD License.

import glob
import os
import sys
import shutil
import subprocess
import unittest

from sos._version import __version__
from sos.parser import SoS_Script
from sos.targets import file_target
from sos.utils import env
# if the test is imported under sos/test, test interacive executor
from sos.workflow_executor import Base_Executor


def multi_attempts(fn):
    def wrapper(*args, **kwargs):
        for n in range(4):
            try:
                fn(*args, **kwargs)
                break
            except Exception:
                if n > 1:
                    raise
    return wrapper


class TestExecute(unittest.TestCase):
    def setUp(self):
        env.reset()
        subprocess.call('sos remove -s', shell=True)
        # self.resetDir('~/.sos')
        self.temp_files = []

    def tearDown(self):
        for f in self.temp_files:
            if file_target(f).exists():
                file_target(f).unlink()

    def touch(self, files):
        '''create temporary files'''
        if isinstance(files, str):
            files = [files]
        #
        for f in files:
            with open(f, 'w') as tmp:
                tmp.write('test')
        #
        self.temp_files.extend(files)

    def resetDir(self, dirname):
        if os.path.isdir(os.path.expanduser(dirname)):
            shutil.rmtree(os.path.expanduser(dirname))
        os.mkdir(os.path.expanduser(dirname))

    def testRerunWithZap(self):
        script = SoS_Script('''
[step_10]
input: for_each={'i': range(3)}
output: f'zapped_example_{i}.txt'
sh: expand=True
  echo "hello" > {_output}

[step_20]
input: group_by=1
output: _input.with_suffix('.bak')
sh: expand=True
   cp {_input} {_output}

_input.zap()
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        #
        script = SoS_Script('''
[step_10]
input: for_each={'i': range(3)}
output: f'zapped_example_{i}.txt'
sh: expand=True
  echo "hello" > {_output}

[step_20]
input: group_by=1
output: _input.with_suffix('.bak')
print(_output)
sh: expand=True
   cp {_input} {_output}

_input.zap()
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        for i in range(3):
            os.remove(f'zapped_example_{i}.txt.zapped')

    def testReturn_OutputInStepOutput(self):
        '''Testing the return of _output as groups of step_output'''
        script = SoS_Script('''\
[1]
input: for_each=dict(i=range(5))
output: f'a_{i}.txt'
_output.touch()
assert(_input.i == i)

[2]
assert(len(step_input.groups) == 5)
assert(len(step_input) == 5)
assert(step_input.groups[0] == 'a_0.txt')
assert(step_input.groups[4] == 'a_4.txt')
#assert(_input.i == _index)
''')
        wf = script.workflow()
        Base_Executor(wf).run()
        #
        # test accumulation of named output
        script = SoS_Script('''\
[1]
input: for_each=dict(i=range(5))
output: a=f'a_{i}.txt', b=f'b_{i}.txt'
_output.touch()

[2]
assert(len(step_input.groups) == 5)
assert(len(step_input) == 10)
assert(step_input.groups[0] == ['a_0.txt', 'b_0.txt'])
assert(step_input.groups[0].sources == ['a', 'b'])
assert(step_input.groups[4] == ['a_4.txt', 'b_4.txt'])
assert(step_input.groups[4].sources == ['a', 'b'])
''')
        wf = script.workflow()
        Base_Executor(wf).run()

    def testOutputFrom(self):
        '''Testing output_from input function'''
        script = SoS_Script('''\
[A]
input: for_each=dict(i=range(5))
output: f'a_{i}.txt'
_output.touch()

[A1]
input: for_each=dict(i=range(4))
output: aa=f'a_{i}.txt'
_output.touch()

[B]
input: output_from('A')
assert(len(step_input.groups) == 5)
assert(len(step_input) == 5)
assert(step_input.sources == ['A']*5)
assert(step_input.groups[0] == 'a_0.txt')
assert(step_input.groups[4] == 'a_4.txt')

[C]
input: K=output_from('A')
assert(len(step_input.groups) == 5)
assert(step_input.sources == ['K']*5)

[D]
input: K=output_from('A', group_by='all')
assert(len(step_input) == 5)
assert(len(step_input.groups) == 1)
assert(step_input.sources == ['K']*5)

[E]
input: output_from('A1', group_by='all')
assert(len(step_input) == 4)
assert(len(step_input.groups) == 1)
assert(step_input.sources == ['aa']*4)

[F]
input: K=output_from('A1', group_by='all')['aa']
assert(len(step_input) == 4)
assert(len(step_input.groups) == 1)
assert(step_input.sources == ['K']*4)

[G_0]
input: for_each=dict(i=range(4))
output: f'g_{i}.txt'
_output.touch()

[G_100]
input: K=output_from(-1, group_by=2)
assert(len(step_input) == 4)
assert(len(step_input.groups) == 2)
assert(step_input.sources == ['K']*4)

[H_0]
input: for_each=dict(i=range(4))
output: f'g_{i}.txt'
_output.touch()

[H_100]
input: K=output_from([-1, 'A1'], group_by=2)
assert(len(step_input) == 8)
assert(len(step_input.groups) == 4)
assert(step_input.sources == ['K']*8)

''')
        for wf in ('B', 'C', 'D', 'E', 'F', 'G', 'H'):
            wf = script.workflow(wf)
            Base_Executor(wf).run()


    def testNamedOutput(self):
        '''Testing named_output input function'''
        script = SoS_Script('''\

[A]
input: for_each=dict(i=range(4))
output: aa=f'a_{i}.txt', bb=f'b_{i}.txt'
_output.touch()

[B]
input: named_output('aa')
assert(len(step_input.groups) == 4)
assert(len(step_input) == 4)
assert(step_input.sources == ['aa']*4)
assert(step_input.groups[0] == 'a_0.txt')
assert(step_input.groups[3] == 'a_3.txt')

[C]
input: K=named_output('bb')
assert(len(step_input.groups) == 4)
assert(len(step_input) == 4)
assert(step_input.sources == ['K']*4)
assert(step_input.groups[0] == 'b_0.txt')
assert(step_input.groups[3] == 'b_3.txt')

[D]
input: K=named_output('bb', group_by=2)
assert(len(step_input.groups) == 2)
assert(len(step_input) == 4)
assert(step_input.sources == ['K']*4)
assert(step_input.groups[1] == ['b_2.txt', 'b_3.txt'])

''')
        for wf in ('B', 'C', 'D'):
            wf = script.workflow(wf)
            Base_Executor(wf).run()

if __name__ == '__main__':
    unittest.main()
