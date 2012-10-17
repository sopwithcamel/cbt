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

src_files = [Glob('src/*.cpp'), Glob('util/*.cpp'), Glob('common/*.cpp')]
cbt_install_headers = Glob('src/*.h')
prefix = '/usr/local'

env.Append(CCFLAGS = ['-g','-O2','-Wall', '-fopenmp'],
            CPPFLAGS = ['-Isrc/', '-Iutil/', '-Icommon'])
cbtlib = env.SharedLibrary('cbt', src_files,
            LIBS = ['-ljemalloc', '-lgomp'])

test_files = ['test/test.pb.cc', 'test/testCBT.cpp']
testapp = env.Program('test/testcbt', test_files,
            LIBS = ['-lgtest', '-lprotobuf', '-lpthread', '-lcbt', '-lsnappy'])

client_files = ['service/Client.cpp', env.Object('common/PartialAgg.cpp'), env.Object('util/HashUtil.cpp')]
client_app = env.Program('service/cbtclient', client_files,
            LIBS = ['-lprotobuf', '-lcbt', '-lsnappy', '-lzmq', '-ldl'])

server_files = ['service/Server.cpp', env.Object('common/PartialAgg.cpp')]
server_app = env.Program('service/cbtserver', server_files,
            LIBS = ['-lprotobuf', '-lcbt', '-lsnappy', '-lzmq', '-lpthread', '-lgflags', '-ljemalloc', '-ldl'])

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

# test
test = env.Alias('test', [testapp])

#service
test = env.Alias('service', [client_app, server_app])
