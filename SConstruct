import os.path

src_files = Glob('src/*.cpp')
cbt_install_headers = Glob('src/*.h')
prefix = '/usr/local'

env = Environment()
env.Append(CCFLAGS = ['-g','-O2','-Wall'])
cbtlib = env.SharedLibrary('cbt', src_files)

# install targets
env.Alias('install-lib', Install(os.path.join(prefix, "lib"), cbtlib))
env.Alias('install-headers', Install(os.path.join(prefix, "include", "cbt"), cbt_install_headers))
env.Alias('install', ['install-lib', 'install-headers'])

