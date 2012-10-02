### SCons build recipe for CBT

# Important targets:
#
# build     - build the software (default)
# install   - install library
#
# audit     - run code-auditing tools
#
# TODO:
# uninstall - undo an install
# check     - run regression and unit tests.


import os.path

env = Environment()

# Utility productions
def Utility(target, source, action):
    target = env.Command(target=target, source=source, action=action)
    env.AlwaysBuild(target)
    env.Precious(target)
    return target

src_files = Glob('src/*.cpp')
cbt_install_headers = Glob('src/*.h')
prefix = '/usr/local'

env.Append(CCFLAGS = ['-g','-O2','-Wall'])
cbtlib = env.SharedLibrary('cbt', src_files)

## Targets
# build targets
build = env.Alias('build', [cbtlib])
env.Default(*build)

# install targets
env.Alias('install-lib', Install(os.path.join(prefix, "lib"), cbtlib))
env.Alias('install-headers', Install(os.path.join(prefix, "include", "cbt"), cbt_install_headers))
env.Alias('install', ['install-lib', 'install-headers'])

# audit
Utility("cppcheck", [], "cppcheck --template gcc --enable=all --force src/")
audit = env.Alias('audit', ['cppcheck'])
